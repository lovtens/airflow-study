from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom_hook.my_hook import MyHook

class HelloOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            name: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.name = name

    def execute(self, context):
        message = "Hello {}".format(self.name)
        hook = MyHook('a')
        print(hook.my_method())
        print(message)
        return message