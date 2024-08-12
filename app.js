const { DATABASE_SCHEMA } = require('./config');
const axios = require('axios');

/**
 * API endpoint to fetch US population data.
 */
const API_URL =
  'https://datausa.io/api/data?drilldowns=Nation&measures=Population';

/**
 * Output string for console messages.
 */
const OUTPUT = 'Population sum (2018-2020) using';

/**
 * Fetches population data from the API.
 * @returns {Promise<Object>} The fetched API data.
 */
async function fetchData() {
  const response = await axios.get(API_URL);
  return response.data;
}

/**
 * Saves the fetched API data to the database.
 * @param {Object} db - Database connection object.
 * @param {Object} apiData - The API data to be saved.
 */
async function saveApiData(db, apiData) {
  await db[DATABASE_SCHEMA].api_data.insert({
    api_name: apiData.source[0].name,
    doc_id: apiData.source[0].annotations.table_id,
    doc_name: apiData.source[0].annotations.dataset_name,
    doc_record: apiData,
  });
  console.log('API data saved successfully.');
}

/**
 * Calculates the population sum for 2018-2020 in-memory using JavaScript.
 * @param {Object} apiData - The API data.
 * @returns {number} The calculated population sum.
 */
function calculatePopulationSumInMemory(apiData) {
  return apiData.data
    .filter((item) => [2020, 2019, 2018].includes(item['ID Year']))
    .reduce((sum, item) => sum + item.Population, 0);
}

/**
 * Calculates the population sum for 2018-2020 using an inline SQL query.
 * @param {Object} db - Database connection object.
 * @returns {Promise<number>} The calculated population sum.
 */
async function calculatePopulationSumInline(db) {
  const result = await db.query(`
    SELECT SUM(CAST((data->>'Population') AS BIGINT)) as total_population
    FROM ${DATABASE_SCHEMA}.api_data,
         jsonb_array_elements(doc_record->'data') as data
    WHERE CAST((data->>'ID Year') AS INTEGER) IN (2018, 2019, 2020)
      AND is_active = true
      AND is_deleted = false
  `);
  return result[0].total_population;
}

/**
 * Retrieves the population sum from a pre-computed database view.
 * @param {Object} db - Database connection object.
 * @returns {Promise<number>} The population sum from the view.
 */
async function getPopulationSumFromView(db) {
  const result = await db.query(
    `SELECT total_population FROM ${DATABASE_SCHEMA}.vw_population_sum`,
  );
  return result[0].total_population;
}

/**
 * Main function orchestrating the entire process.
 * @param {Object} db - Database connection object.
 */
async function main(db) {
  try {
    // Fetch and save API data
    const apiData = await fetchData();
    await saveApiData(db, apiData);

    // Calculate and display population sum using different methods
    const populationSumInMemory = calculatePopulationSumInMemory(apiData);
    console.log(`${OUTPUT} JavaScript:`, populationSumInMemory);

    const populationSumInline = await calculatePopulationSumInline(db);
    console.log(`${OUTPUT} inline SELECT:`, populationSumInline);

    const populationSumFromView = await getPopulationSumFromView(db);
    console.log(`${OUTPUT} VIEW:`, populationSumFromView);
  } catch (error) {
    console.error('Error:', error.message);
  }
}

module.exports = { main };
