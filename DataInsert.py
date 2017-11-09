from flask import Blueprint, render_template, request
import csv
from Auth import Auth
import io
from DB import DBLayer
from psycopg2.extensions import AsIs
from Utils import CSVUtils
import traceback

class DataInsertService:
    DataInsertServiceBlueprint = Blueprint('DataInsertService', 'DataInsertService')

    @staticmethod
    @DataInsertServiceBlueprint.route('/loadCSV', methods=['GET', 'POST'])
    @Auth.auth.login_required
    def serveLoadingPage():
        return render_template("loadCSV.html", title='Load CSV')

    @staticmethod
    @DataInsertServiceBlueprint.route('/uploadfile', methods=['GET', 'POST'])
    def uploadajax():
        try:
            file_val = request.files['file']
            content = file_val.read().decode("ISO-8859-1")
            csv_data = csv.reader(io.StringIO(content), delimiter=',')
            file_val.close()
            queryParams = {}
            queryParams['table'] = AsIs(request.form.get('table'))
            queryParams['schema'] = AsIs(request.form.get('schema'))
            queryParams['AllowUpdate'] = request.form.get('AllowUpdate')
            queryParams['valueArray'] = CSVUtils.CSVUtils.getQueryValuesFromCSV(csv_data)
            # file_val.save(file_val.filename) Will we save the file?

            db = DBLayer.get_DB()
            db.insertBulkRows(queryParams)
        except:
            print (traceback.format_exc())
            return traceback.format_exc(),500
        return 'Successfully uploaded the data!',200

    @staticmethod
    def getQueryValuesFromCSV(csvData):
        valueArr = []
        rowCounter = 0
        for row in csvData:
            if rowCounter != 0:
                value = parseValuesFromRow(row)
                valueArr.append(value)
            rowCounter += 1
        return valueArr

    @staticmethod
    @DataInsertServiceBlueprint.route('/deleteRecords', methods=['GET', 'POST'])
    def deleteRecords():
        try:
            queryParams = {}
            queryParams['table'] = AsIs(request.form.get('table'))
            queryParams['schema'] = AsIs(request.form.get('schema'))
            db = DBLayer.get_DB()
            db.deleteRecords(queryParams)
        except:
            return traceback.format_exc(), 500
        return 'Records deleted successfully'

