from   flask import Flask, flash, request, render_template, redirect
import json
import os
from werkzeug.utils import secure_filename
import logging
from modules.manager.manager_file import ManagerFile
from modules.manager.manager_worker import ManagerWorker
from modules.lotes_site.lotesite import getlotesite
from modules.detail_lote_site.detaillotesite import getdetaillotesite
from monitor import config_logging, profile
import multiprocessing as mp
import config

""" Logging """
config_logging('./logs', 'app', 'INFO')
logger = logging.getLogger()

app = Flask(__name__)
_pool = None
#Extensiones peritidas CSV TXT JSONLINE
ALLOWED_EXTENSIONS = set(config.config.extension_allowed)
app.secret_key = config.config.session_secret
app.config.MAX_CONTENT_LENGTH = 3000 * 1024  * 1024 

  

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/')
def Index():
    return render_template('index.html')


@app.route('/lotesite/', methods=['GET'])
def getLoteSite():
    id= request.args.get('id_lote')
    l_lote = getlotesite(id)
    return render_template('lotesite.html', lotesite = l_lote)


@app.route('/detaillote/', methods=['GET'])
def getDetailLoteSite():
    id_dt = request.args.get('id_dtlote')
    l_detaillote = getdetaillotesite(id_dt)
    return render_template('detaillote.html', detaillote = l_detaillote)

@app.route('/upload')
def upload_form():
    l_encode  = config.config.encode_allowed
    l_tiparch = ALLOWED_EXTENSIONS
    l_sepcol  = config.config.separator_allowed
    l_sepfil  = config.config.separator_allowed
    return render_template('upload.html', list_encode= l_encode,
                        tiparch= l_tiparch,sepcol=l_sepcol,sepfil=l_sepfil)

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'files[]' not in request.files:
        flash('No file part')
        return redirect(request.url)
    files = request.files.getlist('files[]')
    extens_select = request.form['tipoarchivo']
    limitedLine = request.form['separadorfil']
    limitedColumn = request.form['separadorcol']
    encode = request.form['listaencode']
    mf =ManagerFile() 
    filename, extens, file_length = mf.get_info_file(secure_filename,files[0])

    if not allowed_file(files[0].filename):
        flash('Archivo no valido')
        return redirect(request.url)

    if extens != extens_select:
        flash('Archivo no valido con la extension seleccionada')
        return redirect(request.url)

    try: 
        mf.upload_chunks(files[0], filename, extens, limitedLine, limitedColumn,encode)
        total_uploaded = mf.total_uploaded
        
        if total_uploaded != file_length:
            flash('Failed uploaded File(s) ')
            return redirect(request.url)
        id_lote = mf.guarda_header_lote()
        if id_lote is None:
            flash('Error en BD, ver LOG  ')
            return redirect(request.url)            
        run_jobs(id_lote, mf)        
        flash('File successfully uploaded with ID: '+str(id_lote))
    except Exception as e:
        flash(str(e))
    return redirect('/upload')


@app.route('/jobs')
def run_jobs(id_lote, mf):     
    #mf.procesa_file(id_lote) 
    mf.process_lines2(id_lote)
    return render_template('index.html')
      

@app.errorhandler(500)
def handle_500(error):
    return str(error), 500


if __name__ == '__main__':
    app.run(host="0.0.0.0",port=config.config.port,debug=True, use_reloader=False)
    