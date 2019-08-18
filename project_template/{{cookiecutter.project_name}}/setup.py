from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open("requirements.txt", "r") as requirements_file:
    requirements = list(requirements_file.readlines())

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest']

setup(
    author="{{cookiecutter.author}}",
    author_email='{{cookiecutter.author_email}}',
    # description="machine learning pipelines",
    install_requires=requirements,
    # license="GNU General Public License v3",
    long_description=readme,
    include_package_data=True,
    keywords='{{cookiecutter.project_name}}',
    name='{{cookiecutter.project_name}}',
    packages=find_packages(),
    package_dir={'{{ cookiecutter.project_name }}': '{{ cookiecutter.project_name }}'},
    {% if cookiecutter.use_iris_example == 'y' -%}
    entry_points = {
           'console_scripts': [
               '{{ cookiecutter.project_name }}={{ cookiecutter.project_name }}.cli:{{cookiecutter.project_name}}_cli',
           ],
       },
    {%- endif %}
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    version='0.1.0',
    zip_safe=False,
)