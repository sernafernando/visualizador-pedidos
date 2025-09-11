import requests
from lxml import etree
import xml.sax
import html
import pandas as pd
import logging
import re
from flask import render_template_string, current_app
import datetime # Importar datetime al inicio del archivo
import os
from dotenv import load_dotenv

# Cargar variables del archivo .env
load_dotenv()

URL_WS = os.getenv("URL_WS")
P_USERNAME = os.getenv("P_USERNAME")
P_PASSWORD = os.getenv("P_PASSWORD")
P_COMPANY = os.getenv("P_COMPANY")
P_WEBWSERVICE = os.getenv("P_WEBWSERVICE")

TIENDANUBE_STORE_ID = os.getenv("TIENDANUBE_STORE_ID")
TIENDANUBE_ACCESS_TOKEN = os.getenv("TIENDANUBE_ACCESS_TOKEN")
TIENDANUBE_BASE_API_URL = os.getenv("TIENDANUBE_BASE_API_URL")
TIENDANUBE_USER_AGENT = os.getenv("TIENDANUBE_USER_AGENT")


class LargeXMLHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.result_content = []
        self.is_in_result = False
        self.target_tag = 'wsExportDataByIdResult'

    def startElement(self, name, attrs):
        if name == self.target_tag:
            self.is_in_result = True

    def endElement(self, name):
        if name == self.target_tag:
            self.is_in_result = False

    def characters(self, content):
        if self.is_in_result:
            self.result_content.append(content)

class TiendaNubeClient:
    def __init__(self, store_id: str, access_token: str, base_url: str, user_agent: str):
        self.store_id = store_id
        self.base_url = f"{base_url}/{store_id}"
        self.headers = {
            "Authentication": f"bearer {access_token}",
            "User-Agent": user_agent,
            "Content-Type": "application/json"
        }
        logging.info(f"Cliente TiendaNube inicializado para store_id: {self.store_id}")

    def get_order_details(self, order_id: int) -> dict:
        url = f"{self.base_url}/orders/{order_id}"
        logging.info(f"Consultando TiendaNube para orden: {order_id} en {url}")
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            order_data = response.json()
            logging.info(f"Datos de TiendaNube para orden {order_id} obtenidos exitosamente.")
            return order_data
        except requests.exceptions.HTTPError as e:
            logging.error(f"Error HTTP al obtener detalles de la orden {order_id} de TiendaNube: {e.response.status_code} - {e.response.text}")
            return {}
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Error de conexión al intentar acceder a TiendaNube para la orden {order_id}: {e}")
            return {}
        except requests.exceptions.Timeout:
            logging.error(f"Timeout al intentar obtener detalles de la orden {order_id} de TiendaNube.")
            return {}
        except requests.exceptions.RequestException as e:
            logging.error(f"Error desconocido al consultar TiendaNube para la orden {order_id}: {e}")
            return {}
        except Exception as e:
            logging.error(f"Error inesperado en get_order_details para orden {order_id}: {e}")
            return {}


class SoapClient:
    def __init__(self, url_ws, username, password, company, webservice_name):
        self.url_ws = url_ws
        self.pusername = username
        self.ppassword = password
        self.pcompany = company
        self.pwebwervice = webservice_name
        self.token = None
        self.token_acquired_time = None # Nuevo: para almacenar el tiempo de adquisición del token
        self.token_validity_minutes = 55 # Nuevo: Asumimos que el token es válido por 55 minutos (refrescar antes de 1 hora)
        self._authenticate()

    def _authenticate(self):
        soap_action = "http://microsoft.com/webservices/AuthenticateUser"
        xml_payload = (
            f'<?xml version="1.0" encoding="utf-8"?>\n'
            f'<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">\n'
            f'    <soap:Header>\n'
            f'        <wsBasicQueryHeader xmlns="http://microsoft.com/webservices/">\n'
            f'            <pUsername>{self.pusername}</pUsername>\n'
            f'            <pPassword>{self.ppassword}</pPassword>\n'
            f'            <pCompany>{self.pcompany}</pCompany>\n'
            f'            <pBranch>1</pBranch>\n'
            f'            <pLanguage>2</pLanguage>\n'
            f'            <pWebWervice>{self.pwebwervice}</pWebWervice>\n'
            f'        </wsBasicQueryHeader>\n'
            f'    </soap:Header>\n'
            f'    <soap:Body>\n'
            f'        <AuthenticateUser xmlns="http://microsoft.com/webservices/" />\n'
            f'    </soap:Body>\n'
            f'</soap:Envelope>'
        )
        header_ws = {"Content-Type": "text/xml", "SOAPAction": soap_action, "muteHttpExceptions": "true"}

        logging.info("Intentando autenticar con el servicio SOAP...")
        try:
            response = requests.post(self.url_ws, data=xml_payload.encode('utf-8'), headers=header_ws, timeout=30)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            logging.error(f"Timeout durante la autenticación después de 30 segundos.")
            raise ConnectionError("Timeout de autenticación SOAP. El servidor tardó demasiado en responder.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error de red o HTTP durante la autenticación: {e}. Respuesta: {response.text if response is not None else 'No hay respuesta'}")
            raise ConnectionError(f"Error en la solicitud de autenticación SOAP: {e}")

        try:
            root = etree.fromstring(response.content)
            namespaces = {
                'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
                'microsoft': 'http://microsoft.com/webservices/'
            }
            auth_result = root.xpath('//microsoft:AuthenticateUserResult', namespaces=namespaces)

            if auth_result and auth_result[0].text:
                self.token = auth_result[0].text
                self.token_acquired_time = datetime.datetime.now() # Nuevo: Registrar tiempo de adquisición
                logging.info("Autenticación SOAP exitosa. Token obtenido y tiempo registrado.")
            else:
                logging.error("No se encontró el elemento AuthenticateUserResult o estaba vacío en la respuesta de autenticación.")
                logging.debug(f"Respuesta de autenticación completa: {response.text}")
                raise ValueError("No se pudo obtener el token de autenticación SOAP.")
        except etree.XMLSyntaxError as e:
            logging.error(f"Error de sintaxis XML en la respuesta de autenticación: {e}")
            logging.debug(f"Respuesta XML defectuosa: {response.text}")
            raise ValueError("Respuesta de autenticación XML inválida.")
        except Exception as e:
            logging.error(f"Error inesperado al parsear la respuesta de autenticación: {e}")
            logging.debug(f"Respuesta completa: {response.text}")
            raise Exception("Error al procesar la respuesta de autenticación.")


    def get_export_data_by_id(self, int_expgr_id: int, column_mapping: dict, final_columns: list, default_source_name: str) -> list:
        MAX_RETRIES = 1 # Un reintento después del intento inicial
        successful_response = None # Para almacenar la respuesta exitosa

        for attempt in range(MAX_RETRIES + 1):
            # Nuevo: Lógica de refresco proactivo del token
            if self.token is None or \
               (self.token_acquired_time and \
                (datetime.datetime.now() - self.token_acquired_time).total_seconds() / 60 > self.token_validity_minutes):

                logging.info(f"Intento {attempt + 1}: Token SOAP no presente, invalidado, o a punto de expirar. Intentando reautenticar proactivamente...")
                self.token = None # Asegurar que el token se invalide para forzar nueva autenticación
                try:
                    self._authenticate()
                except Exception as e:
                    logging.critical(f"¡ERROR CRÍTICO! Fallo al reautenticar durante el intento {attempt + 1}: {e}. No se puede proceder con la consulta.")
                    return [] # Fallo crítico al autenticar, salimos

            # Si después de intentar autenticar, todavía no tenemos token, salimos
            if self.token is None:
                logging.error(f"Intento {attempt + 1}: No se pudo obtener un token SOAP válido después de reautenticar. Imposible proceder.")
                return []

            soap_action = "http://microsoft.com/webservices/wsExportDataById"
            xml_payload = (
                f'<?xml version="1.0" encoding="utf-8"?>\n'
                f'<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">\n'
                f'    <soap:Header>\n'
                f'        <wsBasicQueryHeader xmlns="http://microsoft.com/webservices/">\n'
                f'            <pUsername>{self.pusername}</pUsername>\n'
                f'            <pPassword>{self.ppassword}</pPassword>\n'
                f'            <pCompany>{self.pcompany}</pCompany>\n'
                f'            <pWebWervice>{self.pwebwervice}</pWebWervice>\n'
                f'            <pAuthenticatedToken>{self.token}</pAuthenticatedToken>\n'
                f'        </wsBasicQueryHeader>\n'
                f'    </soap:Header>\n'
                f'    <soap:Body>\n'
                f'        <wsExportDataById xmlns="http://microsoft.com/webservices/">\n'
                f'            <intExpgr_id>{int_expgr_id}</intExpgr_id>\n'
                f'        </wsExportDataById>\n'
                f'    </soap:Body>\n'
                f'</soap:Envelope>'
            )

            header_ws = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": soap_action, "muteHttpExceptions": "true"}
            REQUEST_TIMEOUT_SECONDS = 120

            logging.info(f"Intento {attempt + 1}/{MAX_RETRIES + 1}: Realizando consulta SOAP a wsExportDataById con intExpgr_id={int_expgr_id}...")
            logging.debug(f"Payload enviado para wsExportDataById: {xml_payload}")

            try:
                response = requests.post(self.url_ws, data=xml_payload.encode('utf-8'), headers=header_ws, timeout=REQUEST_TIMEOUT_SECONDS)
                response.raise_for_status()

                # Analizar el contenido de la respuesta para detectar errores de token SOAP
                if b"<soap:Fault>" in response.content:
                    fault_root = etree.fromstring(response.content)
                    namespaces = {'soap': 'http://schemas.xmlsoap.org/soap/envelope/'}
                    fault_string_elements = fault_root.xpath('//soap:Fault/faultstring/text()', namespaces=namespaces)

                    fault_string = fault_string_elements[0] if fault_string_elements else ""

                    if "Authentication failed" in fault_string or "Invalid token" in fault_string or "Token expired" in fault_string:
                        logging.warning(f"Intento {attempt + 1}: Token SOAP inválido o expirado detectado en el contenido ('{fault_string}'). Invalidando token para reautenticar...")
                        self.token = None # Invalida el token actual
                        if attempt < MAX_RETRIES:
                            continue
                        else:
                            logging.error(f"Todos los {MAX_RETRIES + 1} intentos fallaron por token inválido en el contenido para intExpgr_id={int_expgr_id}.")
                            return []

                logging.info(f"Consulta a wsExportDataById para intExpgr_id={int_expgr_id} exitosa.")
                logging.debug(f"Respuesta RAW para intExpgr_id={int_expgr_id}: {response.content}")
                successful_response = response
                break

            except requests.exceptions.Timeout:
                logging.error(f"Intento {attempt + 1}: La solicitud para intExpgr_id={int_expgr_id} excedió el tiempo límite de {REQUEST_TIMEOUT_SECONDS} segundos.")
                if attempt < MAX_RETRIES:
                    logging.info(f"Reintentando consulta SOAP por timeout...")
                    continue
                else:
                    logging.error(f"Todos los {MAX_RETRIES + 1} intentos de solicitud fallaron por timeout para intExpgr_id={int_expgr_id}.")
                    return []

            except requests.exceptions.RequestException as e:
                response_text = e.response.text if e.response is not None else 'No hay respuesta'
                logging.error(f"Intento {attempt + 1}: Error en la solicitud para intExpgr_id={int_expgr_id}: {e}. Respuesta: {response_text}")
                logging.debug(f"Contenido RAW de la respuesta para intExpgr_id={int_expgr_id}: {e.response.content if e.response is not None else 'N/A'}")

                if e.response is not None and e.response.status_code == 401:
                    logging.warning(f"Intento {attempt + 1}: Error HTTP 401 (Unauthorized) detectado. Token probablemente inválido. Invalidando token para reautenticar...")
                    self.token = None
                    if attempt < MAX_RETRIES:
                        continue
                    else:
                        logging.error(f"Todos los {MAX_RETRIES + 1} intentos fallaron por error HTTP 401 para intExpgr_id={int_expgr_id}.")
                        return []

                if attempt < MAX_RETRIES:
                    logging.info(f"Reintentando consulta SOAP debido a error de red/HTTP no relacionado con autenticación directa...")
                    continue
                else:
                    logging.error(f"Todos los {MAX_RETRIES + 1} intentos de solicitud fallaron por error de red/HTTP para intExpgr_id={int_expgr_id}.")
                    return []

        if successful_response is None:
            logging.error(f"La solicitud a wsExportDataById para intExpgr_id={int_expgr_id} falló después de todos los intentos.")
            return []

        parser = xml.sax.make_parser()
        handler = LargeXMLHandler()
        parser.setContentHandler(handler)
        xml_content = successful_response.content

        try:
            xml.sax.parseString(xml_content, handler)
        except xml.sax.SAXParseException as e:
            logging.error(f"Error al parsear el XML de la respuesta de intExpgr_id={int_expgr_id}: {e}")
            return []
        except Exception as e:
            logging.error(f"Error inesperado al parsear el XML de intExpgr_id={int_expgr_id}: {e}")
            return []

        result_content = ''.join(handler.result_content)
        unescaped_result = html.unescape(result_content)

        df = pd.DataFrame()
        try:
            root_element = etree.fromstring(unescaped_result.encode('utf-8'), parser=etree.XMLParser(recover=True, encoding='utf-8'))

            new_data_set = root_element.find('.//{http://microsoft.com/webservices/}NewDataSet')
            if new_data_set is None:
                new_data_set = root_element.find('.//NewDataSet')

            if new_data_set is None:
                logging.warning("No se encontró el elemento <NewDataSet> dentro del resultado desencapado. Intentando buscar <Table> directamente en la raíz desencapada.")
                table_elements = root_element.xpath('//Table')
            else:
                table_elements = new_data_set.xpath('.//Table')

            data_records = []
            for table in table_elements:
                record = {}
                for child in table:
                    record[child.tag] = child.text if child.text is not None else ''
                data_records.append(record)

            if data_records:
                df = pd.DataFrame(data_records)
                logging.info(f"DataFrame creado por XML (lxml) para intExpgr_id={int_expgr_id} con {df.shape[0]} filas y {df.shape[1]} columnas.")
            else:
                logging.warning(f"No se encontraron elementos <Table> en el XML para intExpgr_id={int_expgr_id}.")
                return []

        except Exception as e:
            logging.error(f"Error al parsear el XML a DataFrame con lxml para intExpgr_id={int_expgr_id}: {e}")
            logging.debug(f"XML defectuoso (inicio): {unescaped_result[:1000]}")
            return []

        df = df.rename(columns=column_mapping)

        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].replace({
                    'NaN': None, 'nan': None, 'None': None, '': None, 'null': None
                })
                df[col] = df[col].apply(lambda x: None if isinstance(x, str) and x.strip() == '' else x)

            if 'fecha' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif col in ['Tipo de Envío', 'Dirección de Envío', 'Observaciones', 'Orden TN', 'NombreCliente', 'Descripción', 'Fuente']:
                df[col] = df[col].astype(str).apply(lambda x: None if x == 'None' or (isinstance(x, str) and x.strip() == '') else x)
            elif df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if pd.api.types.is_float_dtype(df[col]) and df[col].dropna().apply(lambda x: x.is_integer()).all():
                    df[col] = df[col].astype(pd.Int64Dtype())


        df['Fuente'] = default_source_name

        grouped_orders = []
        if not df.empty:
            header_cols = [
                'IDCliente', 'IDPedido', 'Tipo de Envío',
                'Dirección de Envío', 'Observaciones',
                'Fecha de envío', 'Orden TN', 'Fuente', 'NombreCliente', 'orderID'
            ]
            item_cols = ['item_id', 'EAN', 'Descripción', 'Cantidad']

            for pedido_id, group in df.groupby('IDPedido'):
                order_header = {}

                for col in header_cols:
                    if col in group.columns:
                        value = group[col].iloc[0]
                        if pd.isna(value):
                            order_header[col] = None
                        elif pd.api.types.is_numeric_dtype(value):
                            order_header[col] = int(value) if pd.notna(value) else None
                        elif isinstance(value, str):
                            order_header[col] = value
                        elif pd.api.types.is_datetime64_any_dtype(value):
                            order_header[col] = value.isoformat() if pd.notna(value) else None
                        else:
                            order_header[col] = value
                    else:
                        order_header[col] = None

                tiendanube_order_id = None
                if 'orderID' in order_header and order_header['orderID'] is not None:
                    try:
                        tiendanube_order_id = int(float(str(order_header['orderID'])))
                        logging.info(f"Intentando obtener TiendaNube orderID: {tiendanube_order_id} del pedido ID: {pedido_id}")
                    except (ValueError, TypeError) as ve:
                        logging.warning(f"orderID '{order_header['orderID']}' de GlobalBluepoint no es un número válido para TiendaNube. Saltando consulta TN. Error: {ve}")
                        tiendanube_order_id = None

                tn_order_details = None
                if tiendanube_order_id and tiendanube_client:
                    tn_order_details = tiendanube_client.get_order_details(tiendanube_order_id)

                if isinstance(tn_order_details, dict) and tn_order_details and 'shipping_address' in tn_order_details:
                    shipping_address = tn_order_details['shipping_address']
                    logging.info(f"Datos de envío de TiendaNube obtenidos para orden {tiendanube_order_id}.")

                    order_header['telefono_destinatario'] = shipping_address.get('phone')

                    address_parts = [
                        shipping_address.get('address'),
                        shipping_address.get('number'),
                        shipping_address.get('floor')
                    ]
                    order_header['direccion_calle'] = ' '.join(filter(None, address_parts)).strip()

                    order_header['codigo_postal'] = shipping_address.get('zipcode')
                    order_header['barrio'] = shipping_address.get('city')
                    order_header['localidad_tn'] = shipping_address.get('locality')
                    order_header['provincia_tn'] = shipping_address.get('province')
                    order_header['pais_tn'] = shipping_address.get('country')
                    order_header['nombre_destinatario_tn'] = shipping_address.get('name')

                    order_header['tiendanube_order_id'] = tn_order_details.get('id')
                    order_header['tiendanube_order_number'] = tn_order_details.get('number')

                else:
                    logging.warning(f"No se pudieron obtener o no hay datos de envío de TiendaNube para orden {tiendanube_order_id}. Usando datos de GlobalBluepoint como fallback.")

                    direccion_completa_original_gb = order_header.get('Dirección de Envío', "") or ""

                    telefono_destino_gb = ""
                    codigo_postal_gb = ""
                    localidad_gb = ""
                    provincia_gb = ""
                    barrio_destino_gb = ""

                    temp_address_string = direccion_completa_original_gb

                    tel_match_gb = re.search(r'Tel:\+(\d+)', temp_address_string)
                    if tel_match_gb:
                        telefono_destino_gb = '+' + tel_match_gb.group(1)
                        temp_address_string = re.sub(r'Tel:\+\d+', '', temp_address_string).strip()

                    cp_match_gb = re.search(r'\((?P<cp>\d{4,})\)', temp_address_string)
                    if cp_match_gb:
                        codigo_postal_gb = cp_match_gb.group('cp')
                        temp_address_string = re.sub(r'\(\d{4,}\)', '', temp_address_string).strip()

                    provincias_regex = r'(?:Buenos\s*Aires|CABA|Capital\s*Federal|Córdoba|Santa\s*Fe|Mendoza|Tucumán|Salta|Chaco|Corrientes|Entre\s*Ríos|Misiones|Santiago\s*del\s*Estero|Jujuy|San\s*Juan|Río\s*Negro|Neuquén|Formosa|Chubut|San\s*Luis|Catamarca|La\s*Rioja|La\s*Pampa|Santa\s*Cruz|Tierra\s*del\s*Fuego)'

                    loc_prov_match_gb = re.search(r'([^,]+?)\s+(' + provincias_regex + r')$', temp_address_string, re.IGNORECASE)

                    if loc_prov_match_gb:
                        full_loc_prov_part = loc_prov_match_gb.group(0)
                        localidad_gb = loc_prov_match_gb.group(1).strip()
                        provincia_gb = loc_prov_match_gb.group(2).strip()

                        temp_address_string = re.sub(r'\s*' + re.escape(full_loc_prov_part) + r'$', '', temp_address_string, re.IGNORECASE).strip()

                        barrio_destino_gb = localidad_gb
                    else:
                        parts = temp_address_string.split(',')
                        if parts:
                            barrio_destino_gb = parts[-1].strip()
                            temp_address_string = re.sub(re.escape(barrio_destino_gb) + r'\s*$', '', temp_address_string).strip()

                        localidad_gb = barrio_destino_gb
                        provincia_gb = None

                    direccion_calle_final_gb = re.sub(r'\s+', ' ', temp_address_string).strip()

                    order_header['telefono_destinatario'] = telefono_destino_gb
                    order_header['direccion_calle'] = direccion_calle_final_gb
                    order_header['codigo_postal'] = codigo_postal_gb
                    order_header['barrio'] = barrio_destino_gb
                    order_header['localidad_tn'] = localidad_gb
                    order_header['provincia_tn'] = provincia_gb
                    order_header['pais_tn'] = None
                    order_header['nombre_destinatario_tn'] = order_header.get('NombreCliente')


                order_items = []
                total_items_cantidad = 0
                eans_list = []
                for _, item_row in group.iterrows():
                    item_data = {}
                    for col in item_cols:
                        if col in item_row:
                            value = item_row[col]
                            if pd.isna(value):
                                item_data[col] = None
                            elif col == 'Descripción':
                                item_data[col] = str(value) if value is not None else ''
                            elif pd.api.types.is_numeric_dtype(value):
                                item_data[col] = int(value) if pd.notna(value) else None
                            elif isinstance(value, str):
                                try:
                                    item_data[col] = int(float(value))
                                except ValueError:
                                    item_data[col] = value
                            elif pd.api.types.is_datetime64_any_dtype(value):
                                item_data[col] = value.isoformat() if pd.notna(value) else None
                            else:
                                item_data[col] = value
                        else:
                            item_data[col] = None

                    order_items.append(item_data)

                    if 'Cantidad' in item_data and item_data['Cantidad'] is not None:
                        try:
                            total_items_cantidad += int(float(str(item_data['Cantidad'])))
                        except (ValueError, TypeError) as ve:
                            logging.warning(f"Cantidad '{item_data['Cantidad']}' no es un número válido. Error: {ve}. No se sumará a la cantidad total.")
                            pass
                    if 'EAN' in item_data and item_data['EAN'] is not None:
                        eans_list.append(str(item_data['EAN']))

                order_header['Items'] = order_items
                order_header['cantidad_total_items_pedido'] = total_items_cantidad
                order_header['skus_concatenados'] = ', '.join(filter(None, eans_list))

                grouped_orders.append(order_header)

        return grouped_orders

EXPORT_CONFIGS = {
    80: {
        'ws_name': 'wsExportDataById',
        'params': {'intExpgr_id': 80},
        'column_mapping': {
            'IDCliente': 'IDCliente',
            'IDPedido': 'IDPedido',
            'item_id': 'item_id',
            'EAN': 'EAN',
            'Descripción': 'Descripción',
            'Cantidad': 'Cantidad',
            'Tipo_x0020_de_x0020_Envío': 'Tipo de Envío',
            'Dirección_x0020_de_x0020_Envío': 'Dirección de Envío',
            'Observaciones': 'Observaciones',
            'Fecha_x0020_de_x0020_envío': 'Fecha de envío',
            'Orden_x0020_TN': 'Orden TN',
            'NombreCliente': 'NombreCliente',
            'orderID': 'orderID'
        },
        'final_columns': [
            'IDCliente', 'IDPedido', 'item_id', 'EAN', 'Descripción', 'Cantidad',
            'Tipo de Envío', 'Dirección de Envío', 'Observaciones',
            'Fecha de envío', 'Orden TN', 'NombreCliente', 'orderID',
            'telefono_destinatario', 'direccion_calle', 'codigo_postal', 'barrio',
            'localidad_tn', 'provincia_tn', 'pais_tn', 'nombre_destinatario_tn',
            'tiendanube_order_id', 'tiendanube_order_number'
        ],
        'source_name': 'DatosPedidosGlobalBluepointID80'
    },
    104: {
        'ws_name': 'wsExportDataById',
        'params': {'intExpgr_id': 104},
        'column_mapping': {
            'IDCliente': 'IDCliente',
            'IDPedido': 'IDPedido',
            'item_id': 'item_id',
            'EAN': 'EAN',
            'Descripción': 'Descripción',
            'Cantidad': 'Cantidad',
            'Tipo_x0020_de_x0020_Envío': 'Tipo de Envío',
            'Dirección_x0020_de_x0020_Envío': 'Dirección de Envío',
            'Observaciones': 'Observaciones',
            'Fecha_x0020_de_x0020_envío': 'Fecha de envío',
            'Orden_x0020_TN': 'Orden TN',
            'NombreCliente': 'NombreCliente',
            'orderID': 'orderID'
        },
        'final_columns': [
            'IDCliente', 'IDPedido', 'item_id', 'EAN', 'Descripción', 'Cantidad',
            'Tipo de Envío', 'Dirección de Envío', 'Observaciones',
            'Fecha de envío', 'Orden TN', 'NombreCliente', 'orderID',
            'telefono_destinatario', 'direccion_calle', 'codigo_postal', 'barrio',
            'localidad_tn', 'provincia_tn', 'pais_tn', 'nombre_destinatario_tn',
            'tiendanube_order_id', 'tiendanube_order_number'
        ],
        'source_name': 'DatosPedidosGlobalBluepointID104'
    },
}

soap_client = None

def get_soap_client():
    global soap_client
    if soap_client is None:
        try:
            soap_client = SoapClient(URL_WS, P_USERNAME, P_PASSWORD, P_COMPANY, P_WEBWSERVICE)
            logging.info("SoapClient reinicializado con éxito.")
        except Exception as e:
            logging.error(f"No se pudo reinicializar SoapClient dinámicamente: {e}", exc_info=True)
            soap_client = None
    return soap_client


tiendanube_client = None
try:
    tiendanube_client = TiendaNubeClient(
        TIENDANUBE_STORE_ID,
        TIENDANUBE_ACCESS_TOKEN,
        TIENDANUBE_BASE_API_URL,
        TIENDANUBE_USER_AGENT
    )
    logging.info("Cliente TiendaNube inicializado.")
    print(f"DEBUG: tiendanube_client se inicializó como: {tiendanube_client}")
except Exception as e:
    logging.critical(f"¡ERROR CRÍTICO! No se pudo inicializar el cliente TiendaNube al inicio: {e}", exc_info=True)
    print(f"DEBUG: Fallo al inicializar tiendanube_client: {e}")

def process_data_for_export(int_expgr_id):
        client = get_soap_client()
        if not client:
            raise RuntimeError("Cliente SOAP no disponible.")
        processed_orders_list = soap_client.get_export_data_by_id(
        int_expgr_id=int_expgr_id,
        column_mapping=EXPORT_CONFIGS[int_expgr_id]['column_mapping'],
        final_columns=EXPORT_CONFIGS[int_expgr_id]['final_columns'],
        default_source_name=EXPORT_CONFIGS[int_expgr_id]['source_name']
    )

    # ¡IMPORTANTE! Debe devolver la lista completa, no solo el primer elemento.
    # Elimina cualquier '[0]' al final de esta línea de retorno si lo ves.
        return processed_orders_list

def generate_shipping_label_zpl(order_data, total_bultos=1, manual_tipo_envio_etiqueta=None, manual_tipo_domicilio=None):
    zpl_templates_path = 'templates/etiqueta.zpl'

    try:
        with open(f"{current_app.root_path}/{zpl_templates_path}", 'r', encoding='utf-8') as f:
            zpl_template_content = f.read()
    except FileNotFoundError:
        logging.error(f"Plantilla ZPL no encontrada en: {current_app.root_path}/{zpl_templates_path}")
        return []

    # Limpia y procesa Tipo de Envío para la etiqueta
    # Prioriza el valor manual si se proporciona
    tipo_envio_cleaned = manual_tipo_envio_etiqueta
    if tipo_envio_cleaned is None: # Si no se proporcionó manualmente, usa el valor de order_data
        raw_tipo_envio_val = order_data.get('Tipo de Envío')
        if raw_tipo_envio_val is not None:
            # Reemplazar 'x0020' por espacios y limpiar espacios extra
            tipo_envio_cleaned = str(raw_tipo_envio_val).replace('_x0020_', ' ').strip()
        else:
            tipo_envio_cleaned = '' # Default si no hay manual ni en order_data


    # Determinar TIPO_DOMICILIO basado en el tipo de envío
    # Prioriza el valor manual si se proporciona
    tipo_domicilio = manual_tipo_domicilio
    if tipo_domicilio is None: # Si no se proporcionó manualmente, usa la lógica de derivación
        tipo_envio_lower = tipo_envio_cleaned.lower()
        tipo_domicilio = "N/A" # Valor predeterminado si no coincide
        if "domicilio" in tipo_envio_lower:
            tipo_domicilio = "Domicilio"
        elif "sucursal" in tipo_envio_lower:
            tipo_domicilio = "Sucursal"
    # Puedes añadir más mapeos aquí si hay otros tipos de envío y domicilios

    context = {
        'CANTIDAD_ITEMS_PEDIDO': str(order_data.get('cantidad_total_items_pedido') or '0'),
        'SKUS_CONCATENADOS': order_data.get('skus_concatenados') or 'N/A',
        'ID_PEDIDO': str(order_data.get('IDPedido')) or 'N/A',
        'ORDEN_TN': str(order_data.get('tiendanube_order_number') or order_data.get('tiendanube_order_id') or 'N/A'),
        'TIPO_ENVIO_ETIQUETA': tipo_envio_cleaned or 'N/A', # Usamos la versión limpia o manual
        'NOMBRE_DESTINATARIO': order_data.get('nombre_destinatario_tn') or order_data.get('NombreCliente') or 'N/A',
        'TELEFONO_DESTINATARIO': order_data.get('telefono_destinatario') or 'N/A',
        'DIRECCION_CALLE': order_data.get('direccion_calle') or order_data.get('Dirección de Envío') or 'N/A',
        'OBSERVACIONES': order_data.get('Observaciones') or 'N/A',
        'CODIGO_POSTAL': order_data.get('codigo_postal') or 'N/A',
        'BARRIO': order_data.get('barrio') or order_data.get('localidad_tn') or 'N/A',
        'BULTO_ACTUAL': '1', # Se sobreescribe en el bucle
        'TOTAL_BULTOS': str(total_bultos),
        'TIPO_DOMICILIO': tipo_domicilio, # Usamos el valor derivado o manual
        'FUENTE': order_data.get('Fuente', 'N/A'),
        'LOCALIDAD': order_data.get('localidad_tn') or order_data.get('barrio') or 'N/A',
        'PROVINCIA': order_data.get('provincia_tn') or 'N/A',
    }

    generated_labels = []
    for i in range(1, total_bultos + 1):
        label_context = context.copy()
        label_context['BULTO_ACTUAL'] = str(i)

        rendered_zpl = render_template_string(zpl_template_content, **label_context)
        generated_labels.append(rendered_zpl)

    return generated_labels
