from setuptools import setup
from pip.req import parse_requirements


requires = [str(i.req) for i in parse_requirements('requirements.txt',
                                                   session='1')
            if i.req is not None]

setup(name='odq',
      version='0.1',
      packages=['odq'],
      package_data={'odq': ['templates/*', 'static/*/*', 'static/*.*']},
      include_package_data=True,
      install_requires=requires,
      entry_points="""\
      [console_scripts]
      odqw=odq.worker:main
      """)
