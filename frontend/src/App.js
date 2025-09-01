import React, { useState, useEffect, useCallback } from 'react';
import { FaSun, FaMoon } from 'react-icons/fa';
import { FiRefreshCw } from 'react-icons/fi';
import './App.css'; 


function App() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  // exportId ahora es una constante, no un estado modificable por el usuario
  const exportId = '80'; 
  const [theme, setTheme] = useState('dark');
  // Nuevo estado para mostrar mensajes de estado en pantalla
  const [statusMessage, setStatusMessage] = useState('Iniciando carga de datos...'); 

  const toggleTheme = () => {
    setTheme(prevTheme => (prevTheme === 'light' ? 'dark' : 'light'));
  };

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    setStatusMessage('Cargando datos...'); // Actualiza el mensaje de estado al iniciar la carga
    try {
      // Usar ruta relativa para la API, Apache proxyará al backend
      const response = await fetch(`/api/pedidos/${exportId}`);

      if (!response.ok) {
        const errorText = await response.text();
        const errorMessage = `Error HTTP: ${response.status} - ${response.statusText}. Detalles: ${errorText.substring(0, 200)}...`;
        setStatusMessage(`Error al cargar datos: ${errorMessage}`); // Actualiza el mensaje de estado en caso de error HTTP
        throw new Error(errorMessage);
      }

      const result = await response.json();
      if (result.message) {
        setData([]);
        setStatusMessage(result.message); // Muestra el mensaje del backend si no hay datos
        console.warn(result.message);
      } else {
        // Inicializa los datos con valores locales para bultos y tipos de envío/domicilio
        const initializedData = Array.isArray(result) ? result.map(order => ({ 
          ...order, 
          localBultos: 1,
          localTipoEnvioEtiqueta: 'Domicilio', 
          localTipoDomicilio: 'Particular' 
        })) : [{ 
          ...result, 
          localBultos: 1,
          localTipoEnvioEtiqueta: 'Domicilio', 
          localTipoDomicilio: 'Particular' 
        }];
        setData(initializedData);
        setStatusMessage('Datos cargados correctamente.'); // Mensaje de éxito
      }
    } catch (err) {
      setError(err);
      setStatusMessage(`Error de conexión: ${err.message}`); // Actualiza el mensaje de estado en caso de error de red
      console.error("Error al cargar datos:", err);
    } finally {
      setLoading(false);
    }
  }, [exportId]); // exportId es una constante, pero se mantiene en las dependencias por buena práctica

  const handlePrintZPLLabel = async (currentExportId, orderIdToPrint, numBultos, manualTipoEnvio, manualTipoDomicilio) => {
    if (numBultos < 1) {
      alert("La cantidad de bultos debe ser al menos 1.");
      return;
    }

    if (!orderIdToPrint) {
      alert("No se pudo obtener el ID del pedido para imprimir la etiqueta.");
      return;
    }

    try {
      // Usar ruta relativa para la API
      const response = await fetch(`/api/pedidos/label_zpl/${currentExportId}/${orderIdToPrint}/${numBultos}?tipo_envio_etiqueta=${encodeURIComponent(manualTipoEnvio)}&tipo_domicilio=${encodeURIComponent(manualTipoDomicilio)}`);
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Error al obtener la etiqueta ZPL: ${response.status} - ${response.statusText}. Detalles: ${errorText.substring(0, 200)}...`);
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);

      const a = document.createElement('a');
      a.href = url;
      a.download = `etiqueta_pedido_ExpID${currentExportId}_SOH${orderIdToPrint}.txt`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);

      window.URL.revokeObjectURL(url);

    } catch (err) {
      console.error("Error al imprimir la etiqueta ZPL:", err);
      alert(`No se pudo imprimir la etiqueta ZPL: ${err.message}`);
    }
  };

  // Función para actualizar el 'localBultos' de un pedido específico
  const updateLocalBultos = (orderId, newBultos) => {
    setData(prevData =>
      prevData.map(order =>
        order.IDPedido === orderId ? { ...order, localBultos: newBultos } : order
      )
    );
  };

  // Funciones para actualizar localTipoEnvioEtiqueta y localTipoDomicilio
  const updateLocalTipoEnvioEtiqueta = (orderId, newValue) => {
    setData(prevData =>
      prevData.map(order =>
        order.IDPedido === orderId ? { ...order, localTipoEnvioEtiqueta: newValue } : order
      )
    );
  };

  const updateLocalTipoDomicilio = (orderId, newValue) => {
    setData(prevData =>
      prevData.map(order =>
        order.IDPedido === orderId ? { ...order, localTipoDomicilio: newValue } : order
      )
    );
  };

	const handleRefresh = async () => {
	  setLoading(true);
	  setError(null);
	  setStatusMessage("Cargando datos...");
	  try {
	    const res = await fetch(`/api/pedidos/${exportId}`);
	    if (!res.ok) {
	      if (res.status === 500) {
	        const retry = await fetch('/reintentar-cliente-soap', { method: 'POST' });
	        const retryResult = await retry.json();
	        console.log('Resultado de reintento:', retryResult);

	        const res2 = await fetch(`/api/pedidos/${exportId}`);
	        if (!res2.ok) throw new Error("No se pudo recuperar ni después del reintento");

	        const datosFinal = await res2.json();
	        const processed = Array.isArray(datosFinal)
	          ? datosFinal.map(order => ({
	              ...order,
	              localBultos: 1,
	              localTipoEnvioEtiqueta: 'Domicilio',
	              localTipoDomicilio: 'Particular'
	            }))
	          : [{
	              ...datosFinal,
	              localBultos: 1,
	              localTipoEnvioEtiqueta: 'Domicilio',
	              localTipoDomicilio: 'Particular'
	            }];
	        setData(processed);
	        setStatusMessage("Conexión recuperada y datos cargados.");
	        return;
	      }
	      throw new Error("Error del servidor");
	    }

	    const datos = await res.json();
	    const processed = Array.isArray(datos)
	      ? datos.map(order => ({
	          ...order,
	          localBultos: 1,
	          localTipoEnvioEtiqueta: 'Domicilio',
	          localTipoDomicilio: 'Particular'
	        }))
	      : [{
	          ...datos,
	          localBultos: 1,
	          localTipoEnvioEtiqueta: 'Domicilio',
	          localTipoDomicilio: 'Particular'
	        }];
	    setData(processed);
	    setStatusMessage("Datos cargados correctamente.");
	  } catch (error) {
	    console.error("Error al cargar pedidos:", error);
	    setStatusMessage("Error al conectar con el servidor.");
	  } finally {
	    setLoading(false);
	  }
	};


  useEffect(() => {
    fetchData(); // Carga inicial de datos
    // Configura la recarga automática cada 5 minutos (300000 ms)
    const intervalId = setInterval(fetchData, 300000); 
    return () => clearInterval(intervalId); // Limpia el intervalo al desmontar el componente
  }, [fetchData]);

  return (
    <div className={`App ${theme}-theme`}>
      {statusMessage && (<div className="status-bar">{statusMessage}</div>)}
      <header className="App-header">
	  <div className="header-left">
	    <img src={theme === 'dark' ? '/logo-dark.png' : '/logo-light.png'} alt="Logo" className="header-logo" />
	  </div>

	  <div className="header-center">
	    <h2 className="header-title">Visualizador de Pedidos</h2>
	  </div>

	  <div className="header-right">
		<button onClick={handleRefresh} className={`refresh-button ${loading ? 'rotating' : ''}`} title="Refrescar datos">
		  <FiRefreshCw size={20} />
		</button>
	  </div>
      </header>
	<button onClick={toggleTheme} className="theme-floating-button" title="Cambiar tema">
	  {theme === 'light' ? <FaMoon /> : <FaSun />}
	</button>

      <div className="cards-container">
        {data.map((order, index) => (
          <div key={order.IDPedido || index} className="order-card">
            <div className="card-header">
              <span className="order-id">Pedido GBP: {order.IDPedido || 'N/A'}</span>
              {order.IDCliente && <span className="client-info">Cliente GBP: {order.IDCliente}</span>}
              {order['Tipo de Envío'] && <span className={`order-status status-${order['Tipo de Envío'].toLowerCase().replace(/\s/g, '-')}`}>{order['Tipo de Envío']}</span>}
            </div>

            <div className="card-content">
              {order.tiendanube_order_id && (
                <>
                  <p><strong>Pedido TN ID:</strong> {order.tiendanube_order_id}</p>
                  <p><strong>Pedido TN #:</strong> {order.tiendanube_order_number || 'N/A'}</p>
                  <p><strong>Destinatario TN:</strong> {order.nombre_destinatario_tn || 'N/A'}</p>
                  <p><strong>Teléfono TN:</strong> {order.telefono_destinatario || 'N/A'}</p>
                  <p><strong>Dirección TN:</strong> {order.direccion_calle || 'N/A'}</p>
                  <p><strong>Barrio TN:</strong> {order.barrio || 'N/A'}</p>
                  <p><strong>Localidad TN:</strong> {order.localidad_tn || 'N/A'}</p>
                  <p><strong>Provincia TN:</strong> {order.provincia_tn || 'N/A'}</p>
                  <p><strong>Código Postal TN:</strong> {order.codigo_postal || 'N/A'}</p>
                  <p><strong>País TN:</strong> {order.pais_tn || 'N/A'}</p>
                </>
              )}

              {order.NombreCliente && <p><strong>Nombre Cliente GBP:</strong> {order.NombreCliente}</p>}
              {order['Fecha de envío'] && (
                <p>
                  <strong>Fecha de Envío:</strong>{' '}
                  {(() => {
                    const date = new Date(order['Fecha de envío']);
                    return !isNaN(date) ? date.toLocaleDateString() : 'N/A';
                  })()}
                </p>
              )}
              {order.Observaciones && <p><strong>Observaciones:</strong> {order.Observaciones}</p>}
              {order['Dirección de Envío'] && <p><strong>Dirección de Envío GBP:</strong> {order['Dirección de Envío']}</p>}
              <p><strong>Cantidad Total Items:</strong> {order.cantidad_total_items_pedido || 'N/A'}</p>
              <p><strong>SKUs Concatenados:</strong> {order.skus_concatenados || 'N/A'}</p>
            </div>

            {order.Items && order.Items.length > 0 && (
              <div className="order-items-section">
                <h4>Items del Pedido:</h4>
                <ul className="order-items-list">
                  {order.Items.map((item, itemIdx) => (
                    <li key={item.item_id ? `${order.IDPedido}-${item.item_id}-${itemIdx}` : `${order.IDPedido}-item-${itemIdx}`} className="order-item">
                      <p>
                        <strong>Descripción:</strong> {item.Descripción || 'N/A'}
                        <br />
                        <strong>EAN:</strong> {item.EAN || 'N/A'}
                        <br />
                        <strong>Cantidad:</strong> {item.Cantidad || 'N/A'}
                      </p>
                    </li>
                  ))}
                </ul>
              </div>
            )}

	    {/* acá mostramos el error si existe */}
	    {error && <p className="error">{error}</p>}	

            <div className="card-actions">
              <div style={{display: 'flex', flexDirection: 'column', gap: '5px', alignItems: 'flex-start'}}>
                <label htmlFor={`bultos-${order.IDPedido}`}>Bultos:</label>
                <input
                  type="number"
                  id={`bultos-${order.IDPedido}`}
                  min="1"
                  value={order.localBultos}
                  onChange={(e) => updateLocalBultos(order.IDPedido, parseInt(e.target.value))}
                  style={{ width: '60px'}}
                />

                <label htmlFor={`tipoEnvioEtiquetaManual-${order.IDPedido}`}>Tipo Envío Etiqueta:</label>
                <input
                  type="text"
                  id={`tipoEnvioEtiquetaManual-${order.IDPedido}`}
                  value={order.localTipoEnvioEtiqueta}
                  onChange={(e) => updateLocalTipoEnvioEtiqueta(order.IDPedido, e.target.value)}
                  placeholder="Ej: Domicilio"
                />

                <label htmlFor={`tipoDomicilioManual-${order.IDPedido}`}>Tipo Domicilio:</label>
                <select
                  id={`tipoDomicilioManual-${order.IDPedido}`}
                  value={order.localTipoDomicilio}
                  onChange={(e) => updateLocalTipoDomicilio(order.IDPedido, e.target.value)}
                >
                  <option value="Particular">Particular</option>
                  <option value="Comercial">Comercial</option>
                </select>
              </div>
              
              <button 
                onClick={() => handlePrintZPLLabel(
                  exportId, // Ahora es una constante
                  order.IDPedido, 
                  order.localBultos, 
                  order.localTipoEnvioEtiqueta, 
                  order.localTipoDomicilio
                )} 
                className="print-label-button"
              >
                Descargar Etiqueta ZPL
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default App;
