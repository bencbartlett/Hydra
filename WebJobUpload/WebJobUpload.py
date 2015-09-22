# Web interface for Hydra

# To run:
# cd WebJobUpload/
# . venv/bin/activate
# python WebJobUpload.py

import os
from flask import Flask, request, redirect, url_for, render_template
from werkzeug import secure_filename


UPLOAD_FOLDER = '../Server/jobs'
ALLOWED_EXTENSIONS = set(['txt', 'zip', 'png'])

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


@app.route('/', methods=['POST', 'GET'])
def upload_file():
    if request.method == 'POST':
        file = request.files['file']
        if file and allowed_file(file.filename): #TODO: check do_verify, add error page
            filename = secure_filename(file.filename)

            # Write uploaded file to disk
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

            # Write email and timeout to .txt file
            f = open(os.path.join(app.config['UPLOAD_FOLDER'], (filename + ".txt")), 'w')
            f.write(request.form['uploaderEmail'] + os.linesep + request.form['timeoutLength'])
            f.close()

            return "Upload successful, thanks!"
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
