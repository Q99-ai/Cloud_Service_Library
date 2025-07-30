# 
FROM python:3.11

# 
WORKDIR /code

#
RUN pwd

# 
COPY ./requirements.txt /code/requirements.txt

# 
RUN pip install --upgrade -r /code/requirements.txt

COPY ./cloud_services /code/cloud_services
COPY ./tests /code/tests
COPY ./setup.py /code/setup.py
COPY ./cloud_services.egg-info /code/cloud_services.egg-info
COPY ./dist /code/dist
#COPY ./build /code/build