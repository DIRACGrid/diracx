from asyncio import TaskGroup


class ForgivingTaskGroup(TaskGroup):
    # Hacky way, check https://stackoverflow.com/questions/75250788/how-to-prevent-python3-11-taskgroup-from-canceling-all-the-tasks
    # Basically e're using this because we want to wait for all tasks to finish, even if one of them raises an exception
    def _abort(self):
        return None
