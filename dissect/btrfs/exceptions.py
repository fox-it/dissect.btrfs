class Error(Exception):
    pass


class FileNotFoundError(Error):
    pass


class NotAFileError(Error):
    pass


class NotADirectoryError(Error):
    pass


class NotASymlinkError(Error):
    pass
