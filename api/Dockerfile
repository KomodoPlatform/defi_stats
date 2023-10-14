FROM python:3.10 AS python-poetry-base

ARG POETRY_VERSION=""
ENV POETRY_VERSION=${POETRY_VERSION}
ENV POETRY_HOME="/opt/poetry"
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV POETRY_NO_INTERACTION=1
ENV PATH="$POETRY_HOME/bin:$PATH"

FROM python-poetry-base AS python-poetry-builder

WORKDIR /home/app
COPY ./pyproject.toml ./poetry.lock* ./ /home/app/

RUN pip install poetry==1.6.1
RUN poetry install
CMD ["poetry", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7766", "--reload"]
