from flask import Flask, request, jsonify, send_file # ¡Añadir Flask aquí!
from flask_cors import CORS
from backend.data_processor import process_data_for_export, generate_shipping_label_zpl
import io
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__) # Esta línea ahora funcionará
CORS(app) # Habilitar CORS para todas las rutas

@app.route('/')
def hello_world():
    return '¡Hola desde Flask!'

@app.route('/api/pedidos/<int:export_id>', methods=['GET'])
def get_pedidos(export_id):
    logging.info(f"Solicitud recibida para /api/pedidos/{export_id}")
    try:
        # ¡CAMBIO CLAVE! Ahora data es una lista de todos los pedidos
        data = process_data_for_export(export_id)
        if data:
            return jsonify(data) # Devuelve la lista completa
        else:
            return jsonify({"message": "No se encontraron datos para el ID de exportación proporcionado."}), 404
    except Exception as e:
        logging.error(f"Error al procesar datos para export_id {export_id}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/pedidos/label_zpl/<int:export_id>/<int:order_id>/<int:num_bultos>', methods=['GET'])
def get_zpl_label(export_id, order_id, num_bultos):
    logging.info(f"Solicitud de etiqueta ZPL recibida para Pedido SOH: {order_id}, Export ID: {export_id}, Bultos: {num_bultos}")
    
    manual_tipo_envio_etiqueta = request.args.get('tipo_envio_etiqueta', type=str)
    manual_tipo_domicilio = request.args.get('tipo_domicilio', type=str)

    logging.info(f"Parámetros ZPL manuales recibidos: TipoEnvio='{manual_tipo_envio_etiqueta}', TipoDomicilio='{manual_tipo_domicilio}'")

    try:
        # Obtener TODOS los pedidos para el export_id
        all_orders_for_export = process_data_for_export(export_id)
        
        if not all_orders_for_export:
            logging.warning(f"No se encontraron datos para el export_id {export_id} al generar la etiqueta ZPL.")
            return jsonify({"error": "Datos del pedido no encontrados."}), 404
        
        # Buscar el pedido específico por IDPedido dentro de la lista obtenida
        fetched_order_data = next((order for order in all_orders_for_export if order.get('IDPedido') == order_id), None)

        if fetched_order_data is None:
            logging.warning(f"No se encontró el pedido con IDPedido {order_id} en los datos para export_id {export_id}.")
            return jsonify({"error": f"Pedido con ID {order_id} no encontrado para el ID de exportación {export_id}."}), 404


        # Generar la etiqueta ZPL, pasando los parámetros manuales
        zpl_labels = generate_shipping_label_zpl(
            fetched_order_data, 
            total_bultos=num_bultos, 
            manual_tipo_envio_etiqueta=manual_tipo_envio_etiqueta, 
            manual_tipo_domicilio=manual_tipo_domicilio
        )

        if not zpl_labels:
            logging.warning(f"No se generó ninguna etiqueta ZPL para el pedido {order_id}, Export ID {export_id}.")
            return jsonify({"error": "No se pudo generar la etiqueta ZPL."}), 500

        full_zpl_content = "\n".join(zpl_labels)

        return send_file(
            io.BytesIO(full_zpl_content.encode('utf-8')),
            mimetype='text/plain',
            as_attachment=True,
            download_name=f'etiqueta_pedido_ExpID{export_id}_SOH{order_id}.txt'
        )

    except Exception as e:
        logging.error(f"Error al generar la etiqueta ZPL para pedido {order_id}, Export ID {export_id}: {e}", exc_info=True)
        return jsonify({"error": f"Error interno del servidor al generar la etiqueta ZPL: {e}"}), 500

@app.route("/reintentar-cliente-soap", methods=["POST"])
def reiniciar_cliente_soap():
    from backend.data_processor import get_soap_client
    try:
        client = get_soap_client()
        if client is None:
            return jsonify({"status": "error", "mensaje": "No se pudo reinicializar el cliente SOAP."}), 500
        return jsonify({"status": "ok", "mensaje": "Cliente SOAP reiniciado exitosamente."})
    except Exception as e:
        return jsonify({"status": "error", "mensaje": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
