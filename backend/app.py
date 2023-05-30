from flask import Flask ,jsonify, request
from flask_sqlalchemy import SQLAlchemy
from flask_restful import Resource, Api 
import mkwikidata
from flask_cors import CORS


app = Flask(__name__)
app.config ['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///moviedata.sqlite3'
app.app_context().push()
CORS(app, supports_credentials=True)
db = SQLAlchemy(app)
api = Api(app)

class IMBDData(db.Model):
   id = db.Column('imbd_data_id', db.Integer, primary_key = True)
   imdb_id = db.Column(db.String(200))
   movie = db.Column(db.String(200))  
   movie_pubdate = db.Column(db.String(200))

   def __init__(self, imdb_id, movie, movie_pubdate):
    self.imdb_id = imdb_id
    self.movie = movie
    self.movie_pubdate = movie_pubdate


query = """
        SELECT 
            DISTINCT ?imdb_id (MIN(?pubdate) AS ?movie_pubdate) ?itemLabel WHERE {
                ?item wdt:P31 wd:Q11424.
                ?item wdt:P577 ?pubdate.
                ?item wdt:P345 ?imdb_id.
                FILTER(
                    (?pubdate >= "2013-01-01T00:00:00Z"^^xsd:dateTime) && 
                    (STRSTARTS(STR(?imdb_id), "tt")
                )
            )
            SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
            }
            GROUP BY ?imdb_id ?itemLabel
"""

class GetMovieData(Resource):

    def create_records(self):
            data = []
            IMBDData.query.delete()
            query_result = mkwikidata.run_query(query, params={ })

            for value in query_result["results"]["bindings"]:
                imbd_data = IMBDData(
                    value["imdb_id"]["value"],
                    value["itemLabel"]["value"],
                    value["movie_pubdate"]["value"]
                )
                data.append(imbd_data)
                db.session.commit()
            db.session.bulk_save_objects(data)
            db.session.commit()

    def get(self):
        if request.args.get("delete",type=int) == 1:
            self.create_records()
        
        elif not IMBDData.query.all():
            self.create_records()
            
        page = request.args.get("page", 1, type=int)
        per_page = request.args.get("per_page", 10, type=int)
        magazines = IMBDData.query.paginate(page=page, per_page=per_page)

        results = {
            "results": [
                {
                    'id': obj.id, 
                    'imdb_id': obj.imdb_id, 
                    'movie': obj.movie, 
                    'movie_pubdate': obj.movie_pubdate
                } for obj in magazines.items
            ],
            "pagination": {
                "count": magazines.total,
                "page": page,
                "per_page": per_page,
                "pages": magazines.pages,
            },
            }
        return jsonify(results)
api.add_resource(GetMovieData, '/')