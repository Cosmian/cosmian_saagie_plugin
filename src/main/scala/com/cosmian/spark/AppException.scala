package com.cosmian.spark

class AppException(message: String) extends Exception(message) {

    def this(message: String, cause: Throwable) {
        this(message)
        initCause(cause)
    }

    def this(cause: Throwable) {
        this(Option(cause).map(_.toString).orNull, cause)
    }

    def this() {
        this(null: String)
    }
}

object AppException {
    def apply(message: String, t: Throwable) = new AppException(message, t)
    def apply(message: String) = new AppException(message)
}
