// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-db2iseries
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

var g = require('./globalize');

module.exports = mixinDiscovery;

/**
* @param {DB2iSeries} DB2iSeries connector class
* @param {Object} db2i Instance of DB2 iSeries connector
*/
function mixinDiscovery(DB2iSeries, db2i) {
  DB2iSeries.prototype.buildQuerySchemas = function (options) {
    var sql = 'SELECT schema_creator AS "catalog",' +
      ' TRIM(schema_name) AS "schema"' +
      ' FROM qsys2.sysschemas';
    return this.paginateSQL(sql, 'schema_name', options);
  };

  DB2iSeries.prototype.paginateSQL = function (sql, orderBy, options) {
    options = options || {};
    orderBy = orderBy || '1';
    var limitClause = '';
    if (options.offset || options.skip || options.limit) {
      // Offset starts from 0... Where is it ever used?!
      var offset = Number(options.offset || options.skip || 0);
      if (isNaN(offset)) {
        offset = 0;
      }
      if (options.limit) {
        var limit = Number(options.limit);
        if (isNaN(limit)) {
          limit = 0;
        }
        limitClause = ' FETCH FIRST ' + limit + ' ROWS ONLY';
      }
    }
    sql += ' ORDER BY ' + orderBy;
    return sql + limitClause;
  };

  DB2iSeries.prototype.buildQueryTables = function (options) {
    var sqlTables = null;
    var schema = options.owner || options.schema;

    var baseSqlString = 'SELECT \'table\' AS "type",' +
      ' TRIM(table_name) AS "name",' +
      ' TRIM(table_schema) AS "owner"' +
      ' FROM QSYS2.tables';
    if (options.all && !schema) {
      // TODO: What is         ' SUBSTR(property, 20, 1) NOT LIKE \'Y\'',??
      sqlTables = this.paginateSQL(baseSqlString,
        'table_schema, table_name', options);
    } else if (schema) {
      sqlTables = this.paginateSQL(baseSqlString +
        ' WHERE table_schema=\'' + schema + '\'',
        'table_schema, table_name', options);
    } else {
      // TODO: Verify that current tables is correct
      sqlTables = this.paginateSQL(baseSqlString +
        ' WHERE table_schema = CURRENT USER',
        'table_name', options);
    }

    return sqlTables;
  };

  DB2iSeries.prototype.buildQueryViews = function (options) {
    var sqlViews = null;
    if (options.views) {
      var schema = options.owner || options.schema;
      var baseSqlString = 'SELECT \'view\' AS "type",' +
        ' TRIM(table_name) AS "name",' +
        ' TRIM(table_schema) AS "owner"' +
        ' FROM qsys2.tables';
      if (options.all && !schema) {
        sqlViews = this.paginateSQL(baseSqlString,
          'table_schema, table_name', options);
      } else if (schema) {
        sqlViews = this.paginateSQL(baseSqlString +
          ' WHERE table_schema=\'' + schema + '\'',
          'table_schema, table_name', options);
      } else {
        sqlViews = this.paginateSQL(baseSqlString,
          'table_name', options);
      }
    }

    return sqlViews;
  };

  DB2iSeries.prototype.buildQueryColumns = function (schema, table) {
    var sql = null;
    if (schema) {
      sql = this.paginateSQL('SELECT TRIM(table_schema) AS "owner",' +
        ' TRIM(table_name) AS "tableName",' +
        ' TRIM(column_name) AS "columnName",' +
        ' data_type AS "dataType",' +
        ' character_octet_length AS "dataLength",' +
        ' numeric_scale AS "dataScale",' +
        ' (CASE WHEN is_nullable = \'YES\' THEN 1 ELSE 0 END) AS "nullable"' +
        ' FROM qsys2.columns' +
        ' WHERE table_schema = \'' + schema + '\'' +
        (table ? ' AND table_name = \'' + table + '\'' : ''),
        'table_name, ordinal_position', {});
    } else {
      sql = this.paginateSQL('SELECT TRIM(table_schema) AS "owner",' +
        ' TRIM(table_name) AS "tableName",' +
        ' TRIM(column_name) AS "columnName",' +
        ' data_type AS "dataType",' +
        ' character_octet_length AS "dataLength",' +
        ' numeric_scale AS "dataScale",' +
        ' (CASE WHEN is_nullable = \'YES\' THEN 1 ELSE 0 END) AS "nullable"' +
        ' FROM qsys2.syscolumns' +
        (table ? ' WHERE table_name="' + table + '"' : ''),
        'table_name, ordinal_position', {});
    }
    return sql;
  };

  DB2iSeries.prototype.buildPropertyType = function (columnDefinition) {
    var dataType = columnDefinition.dataType;
    var dataLength = columnDefinition.dataLength;

    var type = dataType.toUpperCase();
    switch (type) {
      case 'CHARACTER':
        if (dataLength === 1) {
          // Treat char(1) as boolean
          return 'Boolean';
        } else {
          return 'String';
        }
        break;
      case 'CHARACTER VARYING':
      case 'CHARACTER LARGE OBJECT':
      case 'GRAPHIC':
      case 'GRAPHIC VARYING':
      case 'DOUBLE-BYTE CHARACTER LARGE OBJECT':
      case 'NATIONAL CHARACTER':
      case 'NATIONAL CHARACTER VARYING':
      case 'NATIONAL CHARACTER LARGE OBJECT':
      case 'DATALINK':
      case 'XML':
      case 'USER-DEFINED':
        return 'String';
      case 'BINARY':
      case 'BINARY VARYING':
      case 'BINARY LARGE OBJECT':
        return 'Binary';
      case 'BIGINT':
      case 'INTEGER':
      case 'SMALLINT':
      case 'DECIMAL':
      case 'NUMERIC':
      case 'DOUBLE PRECISION':
      case 'REAL':
      case 'DECFLOAT':
      case 'ROWID':
        return 'Number';
      case 'DATE':
      case 'TIME':
      case 'TIMESTAMP':
        return 'Date';
      default:
        return 'String';
    }
  };

  DB2iSeries.prototype.getArgs = function (table, options, cb) {
    // if ('string' !== (typeof table || !table)) {
    //   throw new Error('table is a required string argument: ' + table);
    // }
    options = options || {};
    // if (!cb && 'function' === (typeof options)) {
    //   cb = options;
    //   options = {};
    // }
    if (typeof options !== 'object') {
      throw new Error(g.f('options must be an {{object}}: %s', options));
    }

    return {
      schema: options.owner || options.schema,
      table: table,
      options: options,
      cb: cb,
    };
  };

  DB2iSeries.prototype.buildQueryPrimaryKeys = function (schema, table) {
    var sql = 'SELECT TRIM(table_schem) AS "owner",' +
      ' TRIM(table_name) AS "tableName",' +
      ' TRIM(column_name) AS "columnName",' +
      ' key_seq AS "keySeq",' +
      ' pk_name AS "pkName"' +
      ' FROM sysibm.sqlprimarykeys' +
      ' WHERE key_seq IS NOT NULL AND key_seq > 0';

    if (schema) {
      sql += ' AND table_schem = \'' + schema + '\'';
    }
    if (table) {
      sql += ' AND table_name = \'' + table + '\'';
    }
    sql += ' ORDER BY' +
      ' table_schem, pk_name, table_name, key_seq';

    return sql;
  };


  DB2iSeries.prototype.buildQueryForeignKeys = function (schema, table) {
    var sql =
      'SELECT TRIM(fktable_schem) AS "fkOwner",' +
      ' TRIM(fk_name) AS "fkName",' +
      ' TRIM(fktable_name) AS "fkTableName",' +
      ' TRIM(pktable_schem) AS "pkOwner",' +
      ' TRIM(pk_name) AS "pkName",' +
      ' TRIM(pktable_name) AS "pkTableName",' +
      ' TRIM(pkcolumn_name) AS "pkColumnName",' +
      ' TRIM(unique_or_primary) AS "parentType"' +
      ' FROM sysibm.sqlforeignkeys';

    if (schema || table) {
      sql += ' WHERE';
      if (schema) {
        sql += ' fktable_schem = \'' + schema + '\' ';
        if (table) sql += ' AND'
      }
      if (table) {
        sql += ' fktable_name LIKE \'' + table + '\'';
      }
    }
    return sql;
  };

  DB2iSeries.prototype.getDefaultSchema = function () {
    return process.env['USER'];
    // if (this.dataSource && this.dataSource.settings &&
    //   this.dataSource.settings.database) {
    //   return this.dataSource.settings.database;
    // }
    // return undefined;
  };

  DB2iSeries.prototype.setDefaultOptions = function (options) {

  };

  DB2iSeries.prototype.setNullableProperty = function (r) {
    r.nullable = r.nullable ? 'Y' : 'N';
  };

  DB2iSeries.prototype.discoverExportedForeignKeys = function (table,
    options, cb) {
    process.nextTick(function () {
      return cb(Error(g.f('Function {{discoverExportedForeignKeys}}' +
        ' not supported')));
    });
  };
}
