FROM python:3.7-slim

WORKDIR /Users/jianhuang/opt/anaconda3/envs/Data26/Data26

COPY requirements.txt ./
RUN cat requirements.txt | xargs -n 1 pip install --no-cache-dir; exit 0

#pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/Users/jianhuang/opt/anaconda3/envs/Data26/Data26

CMD [ "python", "Learning/PyCaret/Test/test_caret.py" ]
