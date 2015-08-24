from odq import Odq

o = Odq()

@o.task
def add(a, b):
    return a + b


@o.task(delay=3)
def add3(a, b):
    return a + b


@o.task(debug=True)
def addn(a, b):
    return a + b


if __name__ == '__main__':
    print(add(1, 2))
    print(add3(2, 3))
    print(addn(1, 3))
    print(add.with_config(delay=60, retry=60)(3, 4))
