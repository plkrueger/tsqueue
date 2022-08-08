;;; thread-safe-q-sbcl.lisp

#|
The MIT license.

Copyright (c) 2022 Paul L. Krueger

Permission is hereby granted, free of charge, to any person obtaining a copy of this software 
and associated documentation files (the "Software"), to deal in the Software without restriction, 
including without limitation the rights to use, copy, modify, merge, publish, distribute, 
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is 
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial 
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT 
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

|#

#|
This thread-safe-queue functionality uses the Bordeaux-threads (bt) library to be
independent of which lisp implementation is being used.
|#

(defpackage :tsq
  (:use :bt :common-lisp)
  (:export
   tsq-debug
   tsq-log10
   tsq-empty-p
   tsq-new-reader
   tsq-pop
   tsq-push-new
   tsq-push
   tsq
   tsq-processing-complete
   tsq-reader-done
   tsq-reader-threads
   tsq-register-reader
   tsq-print-log
   tsq-wait-for-done-or-stalled
   tsq-writer-done
   tsq-writer-threads
   tsq-writers-should-quit))

(in-package :tsq)

;; lockless between readers and writers, so one reader and one writer never stall
;; multiple readers wait for each other
;; multiple writers wait for each other
;; When reader is done it should call tsq-reader-done with tsq as argument
;; When writer is done it should call tsq-writer-done with tsq as argument
;; tsq-writers-should-quit is a convenience if outside process wishes to tell writers to quit
;; writers are free to determine when they should quit on their own, but in eithr case should call
;;   tsq-writer-done
;; when writers are done and tsq is empty, tsq-pop returns :quit and reader should quit
;; when all readers are done, the tsq-processing-complete semaphore is signaled
;; The semaphores tsq-new-reader and tsq-new-writer (tsq slots) are signaled when a new reader or writer is detected
;; interacting with the tsq. They are purely for the convenience of tsq users
;;
;; (tsq-debug self) can be set to t to enable logging of tsq actions as a list of strings in (tsq-log self)

(defclass tsq ()
  ((tsq-first-node :accessor tsq-first-node)
   (tsq-last-node :accessor tsq-last-node)
   (tsq-push-lock :accessor tsq-push-lock
                  :initform (make-lock))
   (tsq-pop-lock :accessor tsq-pop-lock
                 :initform (make-lock))
   (tsq-has-data :accessor tsq-has-data
                 :initform (make-semaphore))
   (tsq-processing-complete :accessor tsq-processing-complete
                            :initform (make-semaphore))
   (tsq-reader-threads :accessor tsq-reader-threads
                     :initform nil)
   (tsq-writer-threads :accessor tsq-writer-threads
                     :initform nil)
   (tsq-new-reader :accessor tsq-new-reader
                   :initform (make-semaphore))
   (tsq-new-writer :accessor tsq-new-writer
                   :initform (make-semaphore))
   (tsq-writers-should-quit :accessor tsq-writers-should-quit
                            :initform nil)
   (tsq-last-activity-time :accessor tsq-last-activity-time
                           :initform 0)
   (tsq-pushed-data :accessor tsq-pushed-data
                    :initform nil)
   (tsq-debug :accessor tsq-debug
              :initform nil)
   (tsq-log-lock :accessor tsq-log-lock
             :initform (make-lock))
   (tsq-log :accessor tsq-log
            :initform nil)))

;; only readers manipulate the tsq-first-node and only writers manipulate the tsq-last-node
;; There is always a first and last node although they may or may not contain valid
;; data (initially they do not). 

(defclass qnode ()
  ((data :accessor data :initarg :data)
   (status :accessor status :initarg :status)
   (tail-p :accessor tail-p :initarg :tail-p)
   (next :accessor next :initarg :next))
  (:default-initargs
      :data nil
      :status :valid
      :tail-p t
      :next nil))

;; data is the data stored in each node
;; status is one of :valid (indicating the node contains valid data),
;; :invalid (indicating the node data field is not valid), or
;; :next-invalid (indicating that the next node contains invalid data
;; even if its own status field is :valid). The :next-invalid staus is
;; only ever set for the first node in the tsq.
;; tail-p indicates that this node is the last in the tsq.
;; next points to the next node in the tsq.

(defmethod initialize-instance :after ((self tsq) &key &allow-other-keys)
  (setf (tsq-last-node self) (make-instance 'qnode
                               :data nil
                               :status :invalid))
  (setf (tsq-first-node self) (make-instance 'qnode
                                :data nil
                                :status :next-invalid
                                :tail-p nil
                                :next (tsq-last-node self))))

(defmethod tsq-log-format ((self tsq) str &rest args)
  (let ((fmt-str (list (apply #'format nil str args))))
    (with-lock-held ((log-lock self))
      (if (tsq-log self)
          (setf (rest (last (tsq-log self))) fmt-str)
          (setf (tsq-log self) fmt-str)))))

(defun tsq-print-log ()
  (with-lock-held ((tsq-log-lock self))
    (format t "~{~a~%~}" (tsq-log self))))

(defmethod tsq-push ((self tsq) new-data)
  (when (tsq-debug self)
    (tsq-log-format self "process ~s pushing ~s on ~s" (current-thread) new-data self))
  (with-lock-held ((tsq-push-lock self))
    ;; block any other writers
    (unless (eq (tsq-writer-threads self) (pushnew (current-thread) (tsq-writer-threads self)))
      (signal-semaphore (tsq-new-writer self)))
    (setf (tsq-last-activity-time self) (get-universal-time))
    (let ((new-node (make-instance 'qnode :data new-data))
          (prev-node (tsq-last-node self)))
      ;; link the new node into the list
      (setf (tsq-last-node self) new-node)
      (setf (next prev-node) new-node)
      ;; reset the tail-p flag which may allow the pop code
      ;; to move the head of the tsq to this node
      (setf (tail-p prev-node) nil))
    ;; tell readers there is something available
    (signal-semaphore (tsq-has-data self))
    new-data))

(defmethod tsq-push-new ((self tsq) new-data &key (test #'eq))
  ;; if tsq-push-new is used to put data on the tsq, it should always be used
  ;; Returns nil if new-data is already in the tsq
  (with-lock-held ((tsq-push-lock self))
    ;; block any other writers
    (unless (eq (tsq-writer-threads self) (pushnew (current-thread) (tsq-writer-threads self)))
      (signal-semaphore (tsq-new-writer self)))
    (unless (member new-data (tsq-pushed-data self) :test test)
      (when (tsq-debug self)
        (tsq-log-format self "process ~s pushing new ~s on ~s" (current-thread) new-data self))
      (setf (tsq-last-activity-time self) (get-universal-time))
      (push new-data (tsq-pushed-data self))
      (let ((new-node (make-instance 'qnode :data new-data))
            (prev-node (tsq-last-node self)))
        ;; link the new node into the list
        (setf (tsq-last-node self) new-node)
        (setf (next prev-node) new-node)
        ;; reset the tail-p flag which may allow the pop code
        ;; to move the head of the tsq to this node
        (setf (tail-p prev-node) nil))
      ;; tell readers there is something available
      (signal-semaphore (tsq-has-data self))
      new-data)))

(defmethod tsq-register-reader ((self tsq))
  (unless (member (current-thread) (tsq-reader-threads self))
    (push (current-thread) (tsq-reader-threads self))
    (signal-semaphore (tsq-new-reader self))))

(defmethod tsq-pop ((self tsq) &key (timeout nil))
  ;; timeout is a real number denoting number of seconds to
  ;; wait for something in the tsq
  (unless (eq (tsq-reader-threads self) (pushnew (current-thread) (tsq-reader-threads self)))
    (signal-semaphore (tsq-new-reader self)))
  (cond ((null (tsq-writer-threads self))
         ;; always do a timed-wait and return :quit if it times out
         (unless (wait-on-semaphore (tsq-has-data self) :timeout 1)
           (when (tsq-debug self)
             (tsq-log-format self "process ~s timeout waiting to pop ~s with no tsq-writer-threads"
                             (current-thread) self))
           (return-from tsq-pop :quit)))
        (timeout
         (unless (wait-on-semaphore (tsq-has-data self) :timeout timeout)
           (when (tsq-debug self)
             (tsq-log-format self "process ~s timeout waiting to pop ~s"
                             (current-thread) self))
           (return-from tsq-pop nil)))
        (t
         ;; to avoid a race condition with the setting of writer-count
         ;; do a timed-wait with a really long timeout and just re-call tsq-pop
         ;; if writers still exist after the timeout
         (unless (wait-on-semaphore (tsq-has-data self) :timeout 15)
           (when (tsq-debug self)
             (tsq-log-format self "process ~s long timeout waiting to pop ~s; will retry"
                             (current-thread) self))
           (return-from tsq-pop (if (tsq-writer-threads self)
                                    (tsq-pop self)
                                    :quit)))))
  (when (tsq-debug self)
    (tsq-log-format self "process ~s popping from ~s" (current-thread) self))
  (with-lock-held ((tsq-pop-lock self))
    ;; block other readers from the tsq
    (setf (tsq-last-activity-time self) (get-universal-time))
    (do* ((fn (tsq-first-node self)
              fn-next)
          (fn-stat (status fn)
                   (status fn))
          (fn-next (next fn)
                   (next fn))
          (next-is-tail (tail-p fn-next)
                        (tail-p fn-next)))
         ((and next-is-tail (eq :next-invalid fn-stat)) nil)
      (if next-is-tail
          ;; since the next node is the tail of the tsq we can't
          ;; shrink the tsq any further
          (case fn-stat
            (:valid 
             ;; the data in the first node is valid
             (setf (status fn) :invalid)
             (return-from tsq-pop (data fn)))
            (:invalid
             ;; the data in the next node is valid
             (setf (status fn) :next-invalid)
             (return-from tsq-pop (data fn-next)))
            (:next-invalid
             ;; The tsq is empty -- actually the "do"
             ;; end condition implements this. We should
             ;; never get to here. And the "do" end-condition
             ;; should never be detected because of the
             ;; semaphore wait.
             (return-from tsq-pop nil)))
          (case fn-stat
            ;; there are more than two nodes in the tsq, so 
            ;; we are permitted to shrink it by moving the first
            ;; pointer to the next node
            (:valid
             ;; the data in the first node is valid
             (setf (status fn) :invalid)
             (return-from tsq-pop (data fn)))
            (:invalid 
             ;; The data here is invalid, but the next node should
             ;; be ok. Shrink the tsq and try again
             (setf (tsq-first-node self) fn-next))
            (:next-invalid 
             ;; Mark the next node as invalid to maintain the information
             ;; contained in the :next-invalid status here and shrink
             ;; the tsq. Try again.
             (setf (status fn-next) :invalid)
             (setf (tsq-first-node self) fn-next)))))))

(defmethod tsq-empty-p ((self tsq))
  (with-lock-held ((tsq-pop-lock self))
    (with-lock-held ((tsq-push-lock self))
      (let* ((fn (tsq-first-node self))
             (nxt (next fn)))
        (and (eq (status fn) :next-invalid)
             (tail-p nxt))))))

(defmethod tsq-reader-done ((self tsq))
  ;; should be called by a reader process before it quits
  (with-lock-held ((tsq-pop-lock self))
    ;; block any other readers
    (setf (tsq-reader-threads self) (delete (current-thread) (tsq-reader-threads self) :test #'equal))
    (unless (tsq-reader-threads self)
      (signal-semaphore (tsq-processing-complete self)))))

(defmethod tsq-writer-done ((self tsq))
  ;; should be called by a writer process before it quite
  (with-lock-held ((tsq-push-lock self))
    ;; block any other writers
    (setf (tsq-writer-threads self) (delete (current-thread) (tsq-writer-threads self)))))

(defmethod tsq-wait-for-done-or-stalled ((self tsq) &optional (stall-delay 10))
  ;; will return only after all processing is complete or there has been no activity within the
  ;; last stall-delay seconds.
  ;; Returns :complete if processing completed normally and :stalled if it did not
  (loop
    thereis (cond ((and (null (tsq-writer-threads self)) (null (tsq-reader-threads self)))
                   :complete)
                  ((>= (- (get-universal-time) (tsq-last-activity-time self)) stall-delay)
                   :stalled)
                  (t
                   nil))
    do (sleep stall-delay)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Test functions

(defparameter *tsq-test-write-count* 0)
(defparameter *tsq-test-read-count* 0)
(defparameter *tsq-test-write-sum* 0)
(defparameter *tsq-test-read-sum* 0)
(defparameter *tsq-test-count-lock* (make-lock))

(defmethod q-writer ((self tsq))
  ;; Wait some random amount of time less than 1/10 sec. and push a random value
  ;; onto the tsq. Continue until told to quit and then report stats to validate
  ;; tsq operation.
  (setf *random-state* (make-random-state t))
  (let ((push-count 0)
        (sum 0))
    (do ((quit (tsq-writers-should-quit self)
               (tsq-writers-should-quit self)))
        (quit (progn
                (with-lock-held (*tsq-test-count-lock*)
                  (incf *tsq-test-write-sum* sum)
                  (incf *tsq-test-write-count* push-count))
                (tsq-writer-done self)))
      (sleep (random 1.0))
      (let ((val (random 10)))
        (tsq-push self val)
        (incf sum val)
        (incf push-count)))))

(defmethod q-reader ((self tsq))
  ;; Pop data off the tsq until told to quit and tsq is empty.
  ;; Then report stats to validate tsq operation.
  (setf *random-state* (make-random-state t))
  (let ((pop-count 0)
        (sum 0))
    (do ((data (tsq-pop self :timeout 2)
               (tsq-pop self :timeout 2)))
        ((eq data :quit)
         (progn
           (with-lock-held (*tsq-test-count-lock*)
             (incf *tsq-test-read-sum* sum)
             (incf *tsq-test-read-count* pop-count))
           (tsq-reader-done self)))
      (when data
        (incf sum data)
        (incf pop-count)))))

(defun test-tsq ()
  (setf *tsq-test-write-count* 0)
  (setf *tsq-test-read-count* 0)
  (setf *tsq-test-read-sum* 0)
  (setf *tsq-test-write-sum* 0)
  (let ((q (make-instance 'tsq)))
    (flet ((reader ()
             (q-reader q))
           (writer ()
             (q-writer q)))
      ;; start multiple reader and writer threads
      (make-thread #'reader :name "reader")
      (make-thread #'reader :name "reader")
      (make-thread #'writer :name "writer")
      (make-thread #'writer :name "writer")
      (make-thread #'writer :name "writer"))
    (sleep (+ 3 (random 7)))
    ;; tell threads to quit
    (setf (tsq-writers-should-quit q) t)
    ;; wait for all the threads to quit
    (unless (wait-on-semaphore (tsq-processing-complete q) :timeout 30)
      (format t "~%They didn't all finish for some reason"))
    ;; report count validity to prove there was one read for one write
    (format t "~%Test wrote ~s and read ~s" *tsq-test-write-count* *tsq-test-read-count*)
    ;; report data consistency to prove that the data read was the same as written
    (format t "~%Test write sum = ~s and read sum = ~s" *tsq-test-write-sum* *tsq-test-read-sum*)
    (values)))
   