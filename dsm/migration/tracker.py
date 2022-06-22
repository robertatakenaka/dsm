from datetime import datetime


class Tracker:

    def __init__(self, tracked_operation):
        self._tracked_operation = tracked_operation
        self._tracks = []

    def info(self, _info):
        self._tracks.append({
            "datetime": datetime.utcnow(),
            "operation": self._tracked_operation,
            "info": _info
        })

    def error(self, _error):
        self._tracks.append({
            "datetime": datetime.utcnow(),
            "operation": self._tracked_operation,
            "error": _error
        })

    @property
    def detail(self):
        return self._tracks

    @property
    def total_errors(self):
        _errors = [event for event in self._tracks if event.get("error")]
        return len(_errors)

    @property
    def status(self):
        return {"status": "failed" if self.total_errors > 0 else "success"}