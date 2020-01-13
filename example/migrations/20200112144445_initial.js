const { setupTables, dropTables } = require('../../dist')

exports.up = function(knex) {
  return setupTables(knex)
};

exports.down = function(knex) {
  return dropTables(knex)
};
