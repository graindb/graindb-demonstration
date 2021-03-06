#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/expression/collate_expression.hpp"

using namespace duckdb;
using namespace std;

string Transformer::TransformCollation(duckdb_libpgquery::PGCollateClause *collate) {
	if (!collate) {
		return string();
	}
	string collation;
	for (auto c = collate->collname->head; c != NULL; c = lnext(c)) {
		auto pgvalue = (duckdb_libpgquery::PGValue *)c->data.ptr_value;
		if (pgvalue->type != duckdb_libpgquery::T_PGString) {
			throw ParserException("Expected a string as collation type!");
		}
		auto collation_argument = string(pgvalue->val.str);
		if (collation.empty()) {
			collation = collation_argument;
		} else {
			collation += "." + collation_argument;
		}
	}
	return collation;
}

unique_ptr<ParsedExpression> Transformer::TransformCollateExpr(duckdb_libpgquery::PGCollateClause *collate) {
	auto child = TransformExpression(collate->arg);
	auto collation = TransformCollation(collate);
	return make_unique<CollateExpression>(collation, move(child));
}

ColumnDefinition Transformer::TransformColumnDefinition(duckdb_libpgquery::PGColumnDef *cdef) {
	SQLType target_type = TransformTypeName(cdef->typeName);
	target_type.collation = TransformCollation(cdef->collClause);

	return ColumnDefinition(cdef->colname, target_type);
}

unique_ptr<CreateStatement> Transformer::TransformCreateTable(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateTableInfo>();

	if (stmt->inhRelations) {
		throw NotImplementedException("inherited relations not implemented");
	}
	assert(stmt->relation);

	info->schema = INVALID_SCHEMA;
	if (stmt->relation->schemaname) {
		info->schema = stmt->relation->schemaname;
	}
	info->table = stmt->relation->relname;
	info->on_conflict = stmt->if_not_exists ? OnCreateConflict::IGNORE : OnCreateConflict::ERROR;
	info->temporary = stmt->relation->relpersistence == duckdb_libpgquery::PGPostgresRelPersistence::PG_RELPERSISTENCE_TEMP;

	if (info->temporary && stmt->oncommit != duckdb_libpgquery::PGOnCommitAction::PG_ONCOMMIT_PRESERVE_ROWS &&
	    stmt->oncommit != duckdb_libpgquery::PGOnCommitAction::PG_ONCOMMIT_NOOP) {
		throw NotImplementedException("Only ON COMMIT PRESERVE ROWS is supported");
	}
	if (!stmt->tableElts) {
		throw ParserException("Table must have at least one column!");
	}

	for (auto c = stmt->tableElts->head; c != NULL; c = lnext(c)) {
		auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(c->data.ptr_value);
		switch (node->type) {
		case duckdb_libpgquery::T_PGColumnDef: {
			auto cdef = (duckdb_libpgquery::PGColumnDef *)c->data.ptr_value;
			auto centry = TransformColumnDefinition(cdef);
			if (cdef->constraints) {
				for (auto constr = cdef->constraints->head; constr != nullptr; constr = constr->next) {
					auto constraint = TransformConstraint(constr, centry, info->columns.size());
					if (constraint) {
						info->constraints.push_back(move(constraint));
					}
				}
			}
			info->columns.push_back(move(centry));
			break;
		}
		case duckdb_libpgquery::T_PGConstraint: {
			info->constraints.push_back(TransformConstraint(c));
			break;
		}
		default:
			throw NotImplementedException("ColumnDef type not handled yet");
		}
	}
	result->info = move(info);
	return result;
}
