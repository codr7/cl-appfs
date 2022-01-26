(defpackage cl-appfs
  (:use cl cl-appfs-db)
  (:export))

(defvar *db*)

(defclass db ()
  ((connection :initarg :connection :reader connection)
   (user-name :initarg :user-name :reader user-name)
   (users :initarg :users :reader users)))

(defun new-db (&key (connection *connection*))
  (let ((db (make-instance 'db :connection connection)))
    (with-slots (user-name users) db
      (setf user-name (new-string-column "user-name"))
      (setf users (new-table '(user-name) user-name)))
    db))

(defmacro with-db ((&rest args) &body body)
  `(let ((*db* (new-db ,@args)))
     ,@body))
