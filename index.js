const dotenv = require("dotenv");
const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const mysql = require("mysql");

dotenv.config();

// Configuración de la conexión a MySQL
const connection = mysql.createConnection({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
});

const folderPath = path.join(__dirname, process.env.PATH_FOLDER);
function formatoToFLoatNumber(Number) {
  let result = parseFloat(Number.replace("$", "").replace(",", ""));
  if (isNaN(result)) {
    return null;
  }
  return result;
}
async function insertRow(row) {
  //  row {
  // 'Trip Number': '98',
  // 'Unit Number': '98',
  // 'Card Number': '21',
  // 'Driver Name': 'IVAN ISAIAS BANDA ZAVALA',
  // 'Driver ID': '',
  // Hub: '5462',
  // VIN: '3WKYD49X9RF540431',
  // 'Merchant Number': '7435',
  // 'Merchant Name': 'TA Laredo',
  // 'Merchant City': 'Laredo',
  // 'Merchant State': 'TX',
  // 'Invoice Number': '51829',
  // 'Invoice Date': '06/16/2023',
  // 'Invoice Time': '03:31:04 PM',
  // 'Diesel 1 Amount': '$0.00',
  // 'Diesel 1 Gallons': '0.0000',
  // 'Diesel 2 Amount': '$594.71',
  // 'Diesel 2 Gallons': '142.9930',
  // 'Regular Unleaded Amount': '$0.00',
  // 'Regular Unleaded Gallons': '0.0000',
  // 'Midrange Unleaded Amount': '$0.00',
  // 'Midrange Unleaded Gallons': '0.0000',
  // 'Premium Unleaded Amount': '$0.00',
  // 'Premium Unleaded Gallons': '0.0000',
  // 'CNG Amount': '$0.00',
  // 'CNG DGEs': '0.0000',
  // 'LNG Amount': '$0.00',
  // 'LNG DGEs': '0.0000',
  // 'LPG Amount': '$0.00',
  // 'LPG Gallons': '0.0000',
  // 'Reefer Amount': '$0.00',
  // 'Reefer Gallons': '0.0000',
  // 'Reefer CNG Amount': '$0.00',
  // 'Reefer CNG DGEs': '0.0000',
  // 'Reefer LNG Amount': '$0.00',
  // 'Reefer LNG DGEs': '0.0000',
  // 'Reefer LPG Amount': '$0.00',
  // 'Reefer LPG Gallons': '0.0000',
  // 'Oil Amount': '$0.00',
  // 'Oil Quantity': '0.0000',
  // 'Additive Amount': '$0.00',
  // 'Minor Repairs': '$0.00',
  // 'Miscellaneous Amount': '$0.00',
  // Tires: '$0.00',
  // 'Day Code': '$0.00',
  // '12 Digit Money': '$0.00',
  // 'E-Money': '$0.00',
  // 'Bulk DEF Amount': '$0.00',
  // 'Bulk DEF Gallons': '0.0000',
  // Tax: '$0.00',
  // Fees: '$0.00',
  // 'Fuel Discount Amount': '-$142.56',
  // 'Total Amount': '$452.15',
  // 'Cents/Gallon Savings': '$1.00',
  // 'Incentive Program': 'No',
  // '': ''
  return new Promise(async (resolve, reject) => {
    let trip_number = row["Trip Number"];
    let unit_number = row["Unit Number"];
    let card_number = row["Card Number"];
    let driver_name = row["Driver Name"];
    let driver_id = row["Driver ID"];
    let hub = row["Hub"];
    let vin = row["VIN"];
    let merchant_number = row["Merchant Number"];
    let merchant_name = row["Merchant Name"];
    let merchant_city = row["Merchant City"];
    let merchant_state = row["Merchant State"];
    let invoice_number = row["Invoice Number"];
    let invoice_date = row["Invoice Date"];
    let invoice_time = row["Invoice Time"];
    let diesel_1_amount = formatoToFLoatNumber(row["Diesel 1 Amount"]);
    let diesel_1_gallons = formatoToFLoatNumber(row["Diesel 1 Gallons"]);
    let diesel_2_amount = formatoToFLoatNumber(row["Diesel 2 Amount"]);
    let diesel_2_gallons = formatoToFLoatNumber(row["Diesel 2 Gallons"]);
    let regular_unleaded_amount = formatoToFLoatNumber(
      row["Regular Unleaded Amount"]
    );
    let regular_unleaded_gallons = formatoToFLoatNumber(
      row["Regular Unleaded Gallons"]
    );
    let midrange_unleaded_amount = formatoToFLoatNumber(
      row["Midrange Unleaded Amount"]
    );
    let midrange_unleaded_gallons = formatoToFLoatNumber(
      row["Midrange Unleaded Gallons"]
    );
    let premium_unleaded_amount = formatoToFLoatNumber(
      row["Premium Unleaded Amount"]
    );
    let premium_unleaded_gallons = formatoToFLoatNumber(
      row["Premium Unleaded Gallons"]
    );
    let cng_amount = formatoToFLoatNumber(row["CNG Amount"]);
    let cng_dges = formatoToFLoatNumber(row["CNG DGEs"]);
    let lng_amount = formatoToFLoatNumber(row["LNG Amount"]);
    let lng_dges = formatoToFLoatNumber(row["LNG DGEs"]);
    let lpg_amount = formatoToFLoatNumber(row["LPG Amount"]);
    let lpg_gallons = formatoToFLoatNumber(row["LPG Gallons"]);
    let reefer_amount = formatoToFLoatNumber(row["Reefer Amount"]);
    let reefer_gallons = formatoToFLoatNumber(row["Reefer Gallons"]);
    let reefer_cng_amount = formatoToFLoatNumber(row["Reefer CNG Amount"]);
    let reefer_cng_dges = formatoToFLoatNumber(row["Reefer CNG DGEs"]);
    let reefer_lng_amount = formatoToFLoatNumber(row["Reefer LNG Amount"]);
    let reefer_lng_dges = formatoToFLoatNumber(row["Reefer LNG DGEs"]);
    let reefer_lpg_amount = formatoToFLoatNumber(row["Reefer LPG Amount"]);
    let reefer_lpg_gallons = formatoToFLoatNumber(row["Reefer LPG Gallons"]);
    let oil_amount = formatoToFLoatNumber(row["Oil Amount"]);
    let oil_quantity = formatoToFLoatNumber(row["Oil Quantity"]);
    let additive_amount = formatoToFLoatNumber(row["Additive Amount"]);
    let minor_repairs = formatoToFLoatNumber(row["Minor Repairs"]);
    let miscellaneous_amount = formatoToFLoatNumber(
      row["Miscellaneous Amount"]
    );
    let tires = formatoToFLoatNumber(row["Tires"]);
    let day_code = formatoToFLoatNumber(row["Day Code"]);
    let _12_digit_money = formatoToFLoatNumber(row["12 Digit Money"]);
    let e_money = formatoToFLoatNumber(row["E-Money"]);
    let bulk_def_amount = formatoToFLoatNumber(row["Bulk DEF Amount"]);
    let bulk_def_gallons = formatoToFLoatNumber(row["Bulk DEF Gallons"]);
    let tax = formatoToFLoatNumber(row["Tax"]);
    let fees = formatoToFLoatNumber(row["Fees"]);
    let fuel_discount_amount = formatoToFLoatNumber(
      row["Fuel Discount Amount"]
    );
    let total_amount = formatoToFLoatNumber(row["Total Amount"]);
    let cents_gallon_savings = formatoToFLoatNumber(
      row["Cents/Gallon Savings"]
    );
    let incentive_program = row["Incentive Program"];
    //06/16/2023 to 2023-06-16
    let invoice_date_format = invoice_date.split("/");
    invoice_date = `${invoice_date_format[2]}-${invoice_date_format[0]}-${invoice_date_format[1]}`; //2023-06-16

    let values = [
      trip_number,
      unit_number,
      card_number,
      driver_name,
      driver_id,
      hub,
      vin,
      merchant_number,
      merchant_name,
      merchant_city,
      merchant_state,
      invoice_number,
      invoice_date,
      invoice_time,
      diesel_1_amount,
      diesel_1_gallons,
      diesel_2_amount,
      diesel_2_gallons,
      regular_unleaded_amount,
      regular_unleaded_gallons,
      midrange_unleaded_amount,
      midrange_unleaded_gallons,
      premium_unleaded_amount,
      premium_unleaded_gallons,
      cng_amount,
      cng_dges,
      lng_amount,
      lng_dges,
      lpg_amount,
      lpg_gallons,
      reefer_amount,
      reefer_gallons,
      reefer_cng_amount,
      reefer_cng_dges,
      reefer_lng_amount,
      reefer_lng_dges,
      reefer_lpg_amount,
      reefer_lpg_gallons,
      oil_amount,
      oil_quantity,
      additive_amount,
      minor_repairs,
      miscellaneous_amount,
      tires,
      day_code,
      _12_digit_money,
      e_money,
      bulk_def_amount,
      bulk_def_gallons,
      tax,
      fees,
      fuel_discount_amount,
      total_amount,
      cents_gallon_savings,
      incentive_program,
    ];
    sql =
      "INSERT INTO diesel_monitor_report( trip_number, unit_number, card_number, driver_name, driver_id, hub, vin, merchant_number, merchant_name, merchant_city, merchant_state, invoice_number, invoice_date, invoice_time, diesel_1_amount, diesel_1_gallons, diesel_2_amount, diesel_2_gallons, regular_unleaded_amount, regular_unleaded_gallons, midrange_unleaded_amount, midrange_unleaded_gallons, premium_unleaded_amount, premium_unleaded_gallons, cng_amount, cng_dges, lng_amount, lng_dges, lpg_amount, lpg_gallons, reefer_amount, reefer_gallons, reefer_cng_amount, reefer_cng_dges, reefer_lng_amount, reefer_lng_dges, reefer_lpg_amount, reefer_lpg_gallons, oil_amount, oil_quantity, additive_amount, minor_repairs, miscellaneous_amount, tires, day_code, _12_digit_money, e_money, bulk_def_amount, bulk_def_gallons, tax, fees, fuel_discount_amount, total_amount, cents_gallon_savings, incentive_program) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    await connection.query(sql, values, function (err, result) {
      if (err) throw reject(err);
    });

    resolve();
  });
  // console.log("Insertando fila:", row);
}

function procesarArchivo(archivoPath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    console.log("Procesando archivo:", archivoPath);

    const tableName = "diesel_monitor_report";
    fs.createReadStream(archivoPath)
      .pipe(csv())
      .on("data", (row) => {
        rows.push(row);
      })
      .on("end", async () => {
        let sql = "DELETE FROM diesel_monitor_report";
        await connection.query(sql, function (err, result) {
          if (err) throw err;
          console.log("Tabla borrada");
        });
        // Procesar las filas una a una
        for (const row of rows) {
          await insertRow(row);
        }
        connection.end();

        resolve();
      })
      .on("error", (error) => {
        reject(error);
      });
  });
}

async function monitorearCarpeta(folderPath) {
  console.log("Monitoreando carpeta:", folderPath);
  fs.readdir(folderPath, (err, files) => {
    if (err) {
      console.error("Error al leer la carpeta:", err);
      return;
    }
    console.log("Archivos encontrados:", files);
    files.forEach(async (file) => {
      const filePath = path.join(folderPath, file);

      // Verificar si el archivo ha terminado de descargarse
      await fs.stat(filePath, async (err, stats) => {
        if (err) {
          console.error("Error al obtener información del archivo:", err);
          return;
        }

        // Verificar si es un archivo regular y no está siendo modificado actualmente
        if (
          stats.isFile() &&
          stats.size > 0 &&
          stats.size === fs.readFileSync(filePath).length
        ) {
          // Procesar el archivo descargado
          await procesarArchivo(filePath);

          // Eliminar el archivo después de ser procesado
          fs.unlink(filePath, (err) => {
            if (err) {
              console.error("Error al eliminar el archivo:", err);
            } else {
              console.log("Archivo eliminado:", filePath);
            }
          });
        }
      });
    });
  });
}

monitorearCarpeta(folderPath);
