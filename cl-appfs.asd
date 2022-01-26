(asdf:defsystem cl-appfs
  :name "cl-appfs"
  :version "1"
  :maintainer "codr7"
  :author "codr7"
  :description "in-app directory/shell"
  :licence "MIT"
  :build-operation "asdf:program-op"
  :build-pathname "clappfs"
  :entry-point "cl-appfs:main"
  :depends-on ("cffi" "local-time")
  :serial t
  :components ((:file "util")
	       (:file "pg")
	       (:file "db")
	       (:file "fs")))
