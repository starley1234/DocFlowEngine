"""Доменные ошибки: HTTP-ошибки входа и ошибки исполнения пайплайна."""


class DocFlowError(Exception):
    status_code = 500
    code = "internal_error"


class NotFoundError(DocFlowError):
    status_code = 404
    code = "not_found"


class ConflictError(DocFlowError):
    status_code = 409
    code = "conflict"


class PayloadTooLargeError(DocFlowError):
    status_code = 413
    code = "payload_too_large"


class UnsupportedMediaTypeError(DocFlowError):
    status_code = 415
    code = "unsupported_media_type"


class PipelineError(Exception):
    """retryable=False означает бессмысленность повторного запуска с теми же данными."""

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


class PermanentPipelineError(PipelineError):
    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=False)


class ProviderError(PipelineError):
    """Ошибка внешнего OCR/LLM/embedding-сервиса."""


class LeaseLostError(Exception):
    """Воркер больше не владеет задачей; результат писать нельзя."""
