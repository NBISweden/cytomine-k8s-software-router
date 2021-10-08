FROM python:3.9-bullseye

#
# Dependencies
#

RUN wget https://github.com/cytomine/Cytomine-python-client/archive/refs/tags/v2.2.2.tar.gz

RUN tar xf v2.2.2.tar.gz

COPY ./src/requirements.txt .

RUN pip install -r requirements.txt
# The cytomine requirements are currently broken, so they are copied to the src
# requirements file for now.
#RUN pip install -r Cytomine-python-client-2.2.2/requirements.txt

RUN pip install Cytomine-python-client-2.2.2/

#
# Clean-up
#

RUN rm v2.2.2.tar.gz
RUN rm -rf Cytomine-python-client-2.2.2

#
# Main application
#

COPY ./src /app

WORKDIR /app

CMD ["python3", "software_router.py"]
