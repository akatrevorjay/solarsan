

# TODO make decorator
def func_to_method(func, cls, method_name=None):
    """Adds func to class so it is an accessible method.
    Use method_name to specify the name to be used for calling the method.
    The new method is accessible to any instance immediately."""
    func.im_class = cls
    func.im_func = func
    func.im_self = None
    if not method_name:
        method_name = func.__name__
    cls.__dict__[method_name] = func
