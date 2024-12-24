from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

app = Flask(__name__)

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('maisto_uzsakymai')


#------------------------------------------------------------------------------
@app.route('/cleanup', methods=['POST'])
def cleanup():
    try:
        session.execute("TRUNCATE maisto_uzsakymai.klientai;")
        session.execute("TRUNCATE maisto_uzsakymai.meniu_patiekalas;")
        session.execute("TRUNCATE maisto_uzsakymai.restoranas;")
        session.execute("TRUNCATE maisto_uzsakymai.uzsakymai;")
        session.execute("TRUNCATE maisto_uzsakymai.uzsakymo_patiekalai;")
        return '', 204
    except Exception as e:
        return jsonify({"error": str(e)}), 500

#------------------------------------------------------------------------------
@app.route('/restoranas', methods=['PUT'])
def put_restoranas():
    data = request.get_json()
    restoranas_id = data.get("restoranas_id")
    pavadinimas = data.get("pavadinimas")
    darbo_laikas = data.get("darbo_laikas")
    adresas = data.get("adresas")

    if not restoranas_id or not pavadinimas or not darbo_laikas or not adresas:
        return jsonify({"error": "Missing required fields: restoranas_id, pavadinimas, darbo_laikas, adresas"}), 400

    query_check = SimpleStatement("SELECT restoranas_id FROM maisto_uzsakymai.restoranas WHERE restoranas_id = %s")
    if session.execute(query_check, (restoranas_id,)).one():
        return jsonify({"error": "Restaurant with this ID already exists"}), 409

    query = SimpleStatement("""
        INSERT INTO maisto_uzsakymai.restoranas (restoranas_id, pavadinimas, darbo_laikas, adresas)
        VALUES (%s, %s, %s, %s) IF NOT EXISTS
    """)
    session.execute(query, (restoranas_id, pavadinimas, darbo_laikas, adresas))
    return jsonify({"restoranas_id": restoranas_id}), 201
#------------------------------------------------------------------------------
@app.route('/restoranas', methods=['GET'])
def get_all_restoranas():
    query = "SELECT * FROM maisto_uzsakymai.restoranas"
    rows = session.execute(query)
    
    restoransai = [{"restoranas_id": row.restoranas_id, "pavadinimas": row.pavadinimas, "darbo_laikas": row.darbo_laikas, "adresas": row.adresas} for row in rows]
    
    if not restoransai:
        return jsonify({"error": "No restaurants found"}), 404
    
    return jsonify(restoransai), 200

#------------------------------------------------------------------------------
@app.route('/klientas', methods=['PUT'])
def put_klientas():
    data = request.get_json()
    klientas_id = data.get("klientas_id")
    vardas = data.get("vardas")
    pavarde = data.get("pavarde")
    telefono_numeris = data.get("telefono_numeris")

    if not klientas_id or not vardas or not pavarde or not telefono_numeris:
        return jsonify({"error": "Missing required fields: klientas_id, vardas, pavarde, telefono_numeris"}), 400

    query_check = SimpleStatement("SELECT klientas_id FROM maisto_uzsakymai.klientai WHERE klientas_id = %s")
    if session.execute(query_check, (klientas_id,)).one():
        return jsonify({"error": "Client with this ID already exists"}), 409

    query = SimpleStatement("""
        INSERT INTO maisto_uzsakymai.klientai (klientas_id, vardas, pavarde, telefono_numeris)
        VALUES (%s, %s, %s, %s) IF NOT EXISTS
    """)
    session.execute(query, (klientas_id, vardas, pavarde, telefono_numeris))
    return jsonify({"klientas_id": klientas_id}), 201

#------------------------------------------------------------------------------
@app.route('/klientas', methods=['GET'])
def get_klientas():
    klientas_id = request.args.get("klientas_id")

    if klientas_id:
        query = SimpleStatement("SELECT * FROM maisto_uzsakymai.klientai WHERE klientas_id = %s")
        rows = session.execute(query, (klientas_id,))
    else:
        query = SimpleStatement("SELECT * FROM maisto_uzsakymai.klientai")
        rows = session.execute(query)

    clients = [{"klientas_id": row.klientas_id, "vardas": row.vardas, "pavarde": row.pavarde, "telefono_numeris": row.telefono_numeris} for row in rows]
    if not clients:
        return jsonify({"error": "Client not found"}), 404
    return jsonify(clients), 200
#---------------------------------------------------------------------------------
@app.route('/meniupatiekalas', methods=['PUT'])
def put_meniu_patiekalas():
    data = request.get_json()
    meniu_patiekalas_id = data.get("meniu_patiekalas_id")
    restoranas_id = data.get("restoranas_id")
    patiekalo_pavadinimas = data.get("patiekalo_pavadinimas")
    aprasymas = data.get("aprasymas")
    iliustracija = data.get("iliustracija")
    kaina = data.get("kaina")

    if not meniu_patiekalas_id or not restoranas_id or not patiekalo_pavadinimas or not aprasymas or not iliustracija or not kaina:
        return jsonify({"error": "Missing required fields"}), 400

    query_check_restoranas = SimpleStatement("SELECT restoranas_id FROM maisto_uzsakymai.restoranas WHERE restoranas_id = %s")
    if not session.execute(query_check_restoranas, (restoranas_id,)).one():
        return jsonify({"error": "Restoranas with the provided restoranas_id does not exist"}), 404

    query_insert = SimpleStatement("""
        INSERT INTO maisto_uzsakymai.meniu_patiekalas (meniu_patiekalas_id, restoranas_id, patiekalo_pavadinimas, aprasymas, iliustracija, kaina)
        VALUES (%s, %s, %s, %s, %s, %s)
    """)
    session.execute(query_insert, (meniu_patiekalas_id, restoranas_id, patiekalo_pavadinimas, aprasymas, iliustracija, kaina))
    return jsonify({"meniu_patiekalas_id": meniu_patiekalas_id}), 201
#---------------------------------------------------------------------------------
@app.route('/meniupatiekalas/<string:restorano_id>/patiekalai', methods=['GET'])
def get_restorano_patiekalai(restorano_id):
    if not restorano_id:
        return jsonify({"error": "Restorano ID is required"}), 400

    query = """
        SELECT meniu_patiekalas_id, restoranas_id, patiekalo_pavadinimas, aprasymas, iliustracija, kaina
        FROM maisto_uzsakymai.meniu_patiekalas_by_restoranas
        WHERE restoranas_id = %s
    """
    rows = session.execute(query, (restorano_id,))
    patiekalai = [
        {
            "meniu_patiekalas_id": row.meniu_patiekalas_id,
            "restoranas_id": row.restoranas_id,
            "patiekalo_pavadinimas": row.patiekalo_pavadinimas,
            "aprasymas": row.aprasymas,
            "iliustracija": row.iliustracija,
            "kaina": row.kaina
        }
        for row in rows
    ]

    if not patiekalai:
        return jsonify({"error": "No dishes found for the given Restorano ID"}), 404

    return jsonify(patiekalai), 200
#---------------------------------------------------------------------------------
@app.route('/uzsakytipatiekalai', methods=['PUT'])
def put_uzsakytipatiekalai():
    data = request.get_json()
    uzsakymo_patiekalai_id = data.get("uzsakymo_patiekalai_id")
    uzsakymas_id = data.get("uzsakymas_id")
    meniu_patiekalas_id = data.get("meniu_patiekalas_id")
    kiekis = data.get("kiekis")

    if not uzsakymo_patiekalai_id or not uzsakymas_id or not meniu_patiekalas_id or not kiekis:
        return jsonify({"error": "Missing required fields"}), 400

    existing_record = session.execute("""
        SELECT uzsakymo_patiekalai_id FROM maisto_uzsakymai.uzsakymo_patiekalai
        WHERE uzsakymo_patiekalai_id = %s
    """, (uzsakymo_patiekalai_id,)).one()

    if existing_record:
        return jsonify({"error": "uzsakymo_patiekalai_id already exists"}), 400

    session.execute("""
        INSERT INTO maisto_uzsakymai.uzsakymo_patiekalai (uzsakymo_patiekalai_id, uzsakymas_id, meniu_patiekalas_id, kiekis)
        VALUES (%s, %s, %s, %s)
    """, (uzsakymo_patiekalai_id, uzsakymas_id, meniu_patiekalas_id, kiekis))
    return '', 204
#---------------------------------------------------------------------------------
@app.route('/uzsakytipatiekalai/<uzsakymas_id>', methods=['GET'])
def get_uzsakytipatiekalai(uzsakymas_id):
    query = "SELECT * FROM maisto_uzsakymai.uzsakymo_patiekalai_by_uzsakymas WHERE uzsakymas_id = %s"
    records = session.execute(query, (uzsakymas_id,))
    results = [dict(record._asdict()) for record in records]

    if not results:
        return jsonify({"error": "Užsakymo patiekalai su šiuo ID nerasti"}), 404

    return jsonify(results), 200
#---------------------------------------------------------------------------------
@app.route('/uzsakymas/<uzsakymas_id>', methods=['PUT'])
def put_uzsakymas_item(uzsakymas_id):
    existing_order = session.execute("""
        SELECT uzsakymas_id FROM maisto_uzsakymai.uzsakymo_patiekalai_by_uzsakymas
        WHERE uzsakymas_id = %s
    """, (uzsakymas_id,)).one()

    if not existing_order:
        return jsonify({"error": f"Order with id {uzsakymas_id} does not exist"}), 404

    data = request.get_json()
    klientas_id = data.get("klientas_id")
    pristatymo_budas = data.get("pristatymo_budas")
    pristatymo_adresas = data.get("pristatymo_adresas")

    if not klientas_id or not pristatymo_budas:
        return jsonify({"error": "Missing required fields"}), 400

    if pristatymo_budas not in ["pristatymas", "atsiimsiu pats"]:
        return jsonify({"error": "Invalid pristatymo_budas value"}), 400

    if pristatymo_budas == "atsiimsiu pats" and pristatymo_adresas:
        return jsonify({"error": "pristatymo_adresas should not be provided for 'atsiimsiu pats'"}), 400

    if pristatymo_budas == "pristatymas" and not pristatymo_adresas:
        return jsonify({"error": "pristatymo_adresas is required for 'pristatymas'"}), 400

    existing_client = session.execute("""
        SELECT klientas_id FROM maisto_uzsakymai.klientai
        WHERE klientas_id = %s
    """, (klientas_id,)).one()

    if not existing_client:
        return jsonify({"error": f"Client with id {klientas_id} does not exist"}), 404

    session.execute("""
        UPDATE maisto_uzsakymai.uzsakymai
        SET klientas_id = %s, pristatymo_budas = %s, pristatymo_adresas = %s
        WHERE uzsakymas_id = %s
    """, (klientas_id, pristatymo_budas, pristatymo_adresas, uzsakymas_id))

    return '', 204

#---------------------------------------------------------------------------------
@app.route('/uzsakymas/<uzsakymas_id>', methods=['GET'])
def get_uzsakymas_total(uzsakymas_id):
    existing_order = session.execute("""
        SELECT * FROM maisto_uzsakymai.uzsakymas_by_id
        WHERE uzsakymas_id = %s
    """, (uzsakymas_id,)).one()

    if not existing_order:
        return jsonify({"error": f"Order with id {uzsakymas_id} does not exist"}), 404

    items = session.execute("""
        SELECT * FROM maisto_uzsakymai.uzsakymo_patiekalai_by_uzsakymas
        WHERE uzsakymas_id = %s
    """, (uzsakymas_id,))

    total_price = 0
    item_details = []
    for item in items:
        menu_item = session.execute("""
            SELECT kaina, patiekalo_pavadinimas FROM maisto_uzsakymai.meniu_patiekalas
            WHERE meniu_patiekalas_id = %s
        """, (item.meniu_patiekalas_id,)).one()

        if menu_item:
            item_total_price = menu_item.kaina * item.kiekis
            total_price += item_total_price
            item_details.append({
                "patiekalo_pavadinimas": menu_item.patiekalo_pavadinimas,
                "kiekis": item.kiekis,
                "kaina": menu_item.kaina,
                "total_kaina": item_total_price
            })

    response_data = {
        "uzsakymas_id": uzsakymas_id,
        "pristatymo_budas": existing_order.pristatymo_budas,
        "klientas_id": existing_order.klientas_id,
        "items": item_details,
        "total_price": total_price
    }

    if existing_order.pristatymo_budas == "atsiimsiu pats" or not existing_order.pristatymo_adresas:
        response_data.pop("pristatymo_adresas", None)
    else:
        response_data["pristatymo_adresas"] = existing_order.pristatymo_adresas

    return jsonify(response_data), 200
#---------------------------------------------------------------------------------
@app.route('/populiarus/<kliento_id>', methods=['GET'])
def get_populiarus(kliento_id):
    past_orders = session.execute("""
        SELECT uzsakymas_id FROM maisto_uzsakymai.uzsakymai_by_klientas
        WHERE klientas_id = %s
    """, (kliento_id,))

    if not past_orders:
        return jsonify({"error": f"No past orders found for client with id {kliento_id}"}), 404

    dish_popularity = {}
    for order in past_orders:
        items = session.execute("""
            SELECT meniu_patiekalas_id, kiekis FROM maisto_uzsakymai.uzsakymo_patiekalai_by_uzsakymas
            WHERE uzsakymas_id = %s
        """, (order.uzsakymas_id,))
        for item in items:
            dish_popularity[item.meniu_patiekalas_id] = dish_popularity.get(item.meniu_patiekalas_id, 0) + item.kiekis

    if not dish_popularity:
        return jsonify({"error": "No dishes found in past orders"}), 404

    top_dishes_ids = sorted(dish_popularity, key=dish_popularity.get, reverse=True)[:3]

    top_dishes = []
    for dish_id in top_dishes_ids:
        dish = session.execute("""
            SELECT patiekalo_pavadinimas FROM maisto_uzsakymai.meniu_patiekalas
            WHERE meniu_patiekalas_id = %s
        """, (dish_id,)).one()

        if dish:
            top_dishes.append({
                "patiekalo_pavadinimas": dish.patiekalo_pavadinimas,
                "uzsakytas_kartus": dish_popularity[dish_id]
            })

    response_data = {
        "klientas_id": kliento_id,
        "top_3_patiekalai": top_dishes
    }

    return jsonify(response_data), 200
#---------------------------------------------------------------------------------
@app.route('/rekomendacija/<kliento_id>', methods=['GET'])
def get_rekomendacija(kliento_id):
    last_order = session.execute("""
        SELECT uzsakymas_id, bendra_kaina, pristatymo_adresas, pristatymo_budas
        FROM maisto_uzsakymai.uzsakymai_by_klientas
        WHERE klientas_id = %s
        ORDER BY uzsakymas_id DESC
        LIMIT 1
    """, (kliento_id,)).one()

    if not last_order:
        return jsonify({"error": f"No orders found for client with id {kliento_id}"}), 404

    items = session.execute("""
        SELECT meniu_patiekalas_id, kiekis FROM maisto_uzsakymai.uzsakymo_patiekalai_by_uzsakymas
        WHERE uzsakymas_id = %s
    """, (last_order.uzsakymas_id,))

    item_details = []
    total_cost = 0
    for item in items:
        menu_item = session.execute("""
            SELECT patiekalo_pavadinimas, kaina FROM maisto_uzsakymai.meniu_patiekalas
            WHERE meniu_patiekalas_id = %s
        """, (item.meniu_patiekalas_id,)).one()

        if menu_item:
            item_total_cost = menu_item.kaina * item.kiekis
            total_cost += item_total_cost
            item_details.append({
                "patiekalo_pavadinimas": menu_item.patiekalo_pavadinimas,
                "kiekis": item.kiekis,
                "kaina": menu_item.kaina,
                "total_kaina": item_total_cost
            })

    response_data = {
        "klientas_id": kliento_id,
        "message": "Ar norite pakartoti paskutinį užsakymą?",
        "last_uzsakymas": {
            "uzsakymas_id": last_order.uzsakymas_id,
            "total_cost": total_cost,
            "pristatymo_budas": last_order.pristatymo_budas,
            "items": item_details
        }
    }

    if not last_order.pristatymo_adresas:
        response_data["last_uzsakymas"].pop("pristatymo_adresas", None)
    else:
        response_data["last_uzsakymas"]["pristatymo_adresas"] = last_order.pristatymo_adresas

    if not last_order.bendra_kaina:
        response_data["last_uzsakymas"].pop("bendra_kaina", None)

    return jsonify(response_data), 200

#---------------------------------------------------------------------------------


if __name__ == '__main__':
    app.run(debug=True)
