import pandas as pd
from flask import Flask
from flask_restplus import  Resource, Api

app = Flask(__name__)
api = Api(app)

@api.route('/books/<int:id>')
class Books(Resource):
    def get(self,id):
        if id not in df.index:
            api.abort(404,"Book {} does not exist".format(id))

        book = dict(df.loc[id])
        return book



if __name__ == "__main__":
    columns_to_drop = ['Edition Statement',
                       'Corporate Author',
                       'Corporate Contributors',
                       'Former owner',
                       'Engraver',
                       'Contributors',
                       'Issuance type',
                       'Shelfmarks'
                       ]
    csv_file = 'Books.csv'
    df = pd.read_csv(csv_file)

    #drop unnecessary columns
    df.drop(columns_to_drop,inplace=True,axis=1)

    #clean the data of publication & convert it to numeric data
    new_data = df['Date of Publication'].str.extract(r'^(\d{4})',expand=False)
    new_data = pd.to_numeric(new_data)
    new_data = new_data.fillna(0)
    df['Date of Publication'] = new_data

    #replace spaces in the name of columns
    df.columns = [c.replace(' ','_') for c in df.columns]

    #set the index column; this will help us to find books with their ids
    df.set_index("Identifier", inplace=True)

    #run the application
    app.run(debug=True)
