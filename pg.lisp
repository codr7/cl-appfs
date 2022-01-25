(defpackage cl-appfs-pg
  (:use cffi cl)
  (:export))

(in-package cl-appfs-pg)

(define-foreign-library libpq (t (:default "/usr/local/opt/libpq/lib/libpq")))

(use-foreign-library libpq)

(defctype PGconn :pointer)

(defcenum ConnStatusType :CONNECTION_OK :CONNECTION_BAD)

;; PGconn *PQconnectdb(const char *conninfo)
(defcfun "PQconnectdb" PGconn (conninfo :string))

;; void PQfinish(PGconn *conn)
(defcfun "PQfinish" :void (conn PGconn))

;;ConnStatusType PQstatus(const PGconn *conn)
(defcfun "PQstatus" ConnStatusType (conn PGconn))

(defun test ()
  (let* ((c (PQconnectdb ""))
	 (s (PQstatus c)))
    (format t "status: ~a~%" s)
    (PQfinish c)))

;;PQsendQueryParams
;;PQsendPrepare
;;PQsendQueryPrepared
;;PQgetResult
