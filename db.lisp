(defpackage cl-appfs-db
  (:use cffi cl cl-appfs-pg)
  (:import-from cl-appfs-util str!)
  (:export *connection* definition exists? new-column new-table table with-db))

(in-package cl-appfs-db)

(defvar *connection*)
(defparameter *debug* t)

(defun connection-ok? (&key (connection *connection*))
  (eq (PQstatus connection) :CONNECTION_OK))

(defun connect (db user password &key (host "localhost"))
  (let ((c (PQconnectdb (format nil "postgresql://~a:~a@~a/~a" user password host db))))
    (unless (connection-ok? :connection c)
      (error (PQerrorMessage c)))
    c))

(defun send-query (sql params &key (connection *connection*))
  (when *debug*
    (format t "~a~%" sql)
    (when params
      (format t "~a~%" params)))

  (let ((nparams (length params)))
    (with-foreign-object (cparams :pointer nparams)
      (let ((i 0))
	(dolist (p params)
	  (setf (mem-aref cparams :pointer i) (foreign-string-alloc p))
	  (incf i)))
      
      (unless (= (PQsendQueryParams connection
				    sql
				    nparams
				    (null-pointer) cparams (null-pointer) (null-pointer)
				    0)
		 1)
	(error (PQerrorMessage connection)))
      
      (dotimes (i (length params))
	(foreign-string-free (mem-aref cparams :pointer i))))))

(defun get-result (&key (connection *connection*))
  (let* ((r (PQgetResult connection)))
    (if (null-pointer-p r)
	(values nil nil)
	(let ((s (PQresultStatus r)))
	  (unless (or (eq s :PGRES_COMMAND_OK) (eq s :PGRES_TUPLES_OK))
	    (error "~a~%~a" s (PQresultErrorMessage r)))
	  (values r s)))))

(defmacro with-connection ((&rest args) &body body)
  `(let ((*connection* (connect ,@args)))
     ,@body
     (PQfinish *connection*)))

(defclass definition ()
  ((name :initarg :name :initform (error "missing name") :reader name)))

(defclass column (definition)
  ())

(defclass string-column (column)
  ())

(defun new-string-column (name)
  (make-instance 'string-column :name name))

(defmethod data-type ((self string-column))
  "TEXT")

(defclass relation ()
  ((columns :initform (make-array 0 :element-type 'column :fill-pointer 0))
   (column-lookup :initform (make-hash-table))))

(defclass key (definition relation)
  ())

(defun new-key (name cols)
  (let ((key (make-instance 'key :name name)))
    (with-slots (columns column-lookup) key
      (dolist (c cols)
	(setf (gethash (name c) column-lookup) (length columns))
	(push c columns)))))

(defclass table (definition relation)
  ((primary-key :reader primary-key)))

(defun new-table (name primary-cols &rest cols)
  (let ((tbl (make-instance 'table :name name)))
    (with-slots (columns column-lookup primary-key) tbl
      (dolist (c cols)
	(setf (gethash (name c) column-lookup) (length columns))
	(vector-push-extend c columns))
      
      (setf primary-key
	    (new-key (intern (format nil "~a-primary" name))
		     (mapcar (lambda (c) (aref columns (gethash c column-lookup))) primary-cols))))
    tbl))

(defun decode-bool (val)
  (string= val "t"))

(defun table-exists? (self)
  (send-query "SELECT EXISTS (
                 SELECT FROM pg_tables
                 WHERE tablename  = $1
               )"
	      (list (str! (name self))))
  (let ((r (get-result)))
    (assert (= (PQntuples r) 1))
    (assert (= (PQnfields r) 1))
    (let ((result (decode-bool (PQgetvalue r 0 0))))
      (PQclear r)
      (assert (null (get-result)))
      result)))

(defmethod exists? ((self table))
  (table-exists? self))

(defun create-table (self)
  (let ((sql (with-output-to-string (out)
	       (format out "CREATE TABLE ~a (" (str! (name self)))
	       (with-slots (columns) self
		 (dotimes (i (length columns))
		   (let ((c (aref columns i)))
		     (unless (eq c (aref columns 0))
		       (format out ", "))
		     (format out "~a ~a" (str! (name c)) (data-type c))))
		 (format out ")")))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defmethod create ((self table))
  (create-table self))

(defun drop-table (self)
  (let ((sql (format nil "DROP TABLE ~a" (str! (name self)))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defmethod drop ((self table))
  (drop-table self))

(defun insert-record (tbl &rest rec)
  (let ((sql (with-output-to-string (out)
	       (format out "INSERT INTO ~a (" (str! (name tbl)))
	       (with-slots (column-lookup) tbl
		 (dolist (f rec)
		   (let ((c (first f)))
		     (unless (gethash c column-lookup)
		       (error "unknown column: ~a" c))
		     (unless (eq f (first rec))
		       (format out ", "))
		     (format out "~a" (str! (name c)))))
		 (format out ")"))
	       (format out ") VALUES (")
	       (dotimes (i (length rec))
		 (unless (zerop i)
		   (format out ", "))
		 (format out "$~a" (1+ i)))
	       (format out ")"))))
    (send-query sql (mapcar #'rest rec)))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defun update-record (tbl &rest rec)
  (let ((sql (with-output-to-string (out)
	       (format out "UPDATE ~a SET " (str! (name tbl)))
	       (let ((param-count 0))
		 (with-slots (column-lookup columns) tbl
		   (dolist (f rec)
		     (let ((c (first f)))
		       (unless (gethash c column-lookup)
			 (error "unknown column: ~a" c))
		       (unless (eq f (first rec))
			 (format out ", "))
		       (format out "~a=$~a" (str! (name c)) (incf param-count)))))
		 (format out " WHERE "))
	       (let ((i 0))
		 (dolist (f rec)
		   (unless (zerop i)
		     (format out " AND "))
		   (format out "~a=$~a" (str! (name (first f))) (incf i))))
	       (format out ")"))))
    (send-query sql (mapcar #'rest rec)))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defun tests ()
  (with-connection ("test" "test" "test")
    (when (not (connection-ok?))
      (error (PQerrorMessage *connection*)))

    (send-query "SELECT * FROM pg_tables" '())
    
    (let* ((r (get-result)))
      (assert (eq (PQresultStatus r) :PGRES_TUPLES_OK))
      (PQclear r))
    (assert (null (get-result)))
    
    (let ((tbl (new-table 'foo '(bar) (new-string-column 'bar))))
      (assert (not (exists? tbl)))
      (create tbl)
      (assert (exists? tbl))
      (drop tbl)
      (assert (not (exists? tbl))))))
