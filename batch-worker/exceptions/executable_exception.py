class ExecutableError(Exception):
    """
    Custom exception class for handling errors during script execution.
    """

    def __init__(self, job_id, command, status_code, output, error_details=None):
        self.job_id = job_id
        self.status_code = status_code
        self.command = command
        self.error_details = error_details
        self.output = output
        super().__init__(self._generate_message())

    def _generate_message(self):
        return f'{self.error_details if self.error_details else self.output}'
