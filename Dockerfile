FROM python:3.10

RUN pip install --upgrade pip

RUN adduser nonroot
RUN mkdir /home/app && chown -R nonroot:nonroot /home/app
WORKDIR /home/app
RUN mkdir /home/app/uploads
RUN mkdir /home/app/uploads/xml
RUN chown -R nonroot:nonroot /home/app/uploads
USER nonroot

COPY --chown=nonroot:nonroot . .
ENV VIRTUAL_ENV=/home/app/venv

RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN pip install -r requirements.txt