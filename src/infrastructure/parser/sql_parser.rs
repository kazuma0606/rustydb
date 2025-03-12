use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::ast::{Statement, Query, SetExpr, TableFactor, Values, Expr, Value as SqlValue, 
                     SelectItem, ObjectName, Ident, TableWithJoins};

use crate::domain::entity::{DataType, Column, Value};
use crate::domain::repository::{FilterCondition, FilterOperator};
use thiserror::Error;

/// SQL解析エラー
#[derive(Error, Debug)]
pub enum ParseError {
    #[error("SQL syntax error: {0}")]
    SyntaxError(String),
    
    #[error("Unsupported SQL feature: {0}")]
    UnsupportedFeature(String),
    
    #[error("Invalid data type: {0}")]
    InvalidDataType(String),
    
    #[error("Invalid value: {0}")]
    InvalidValue(String),
    
    #[error("Internal parser error: {0}")]
    InternalError(String),
}

impl From<ParserError> for ParseError {
    fn from(err: ParserError) -> Self {
        ParseError::SyntaxError(err.to_string())
    }
}

/// SQLパーサーの実装
pub struct SqlParser {
    dialect: GenericDialect,
}

/// CREATE TABLE文からの解析結果
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<Column>,
    pub if_not_exists: bool,
}

/// SELECT文からの解析結果
pub struct SelectStatement {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub filter: Option<FilterCondition>,
    pub limit: Option<usize>,
}

/// INSERT文からの解析結果
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Value>>,
}

/// UPDATE文からの解析結果
pub struct UpdateStatement {
    pub table_name: String,
    pub updates: Vec<(String, Value)>,
    pub filter: Option<FilterCondition>,
}

/// DELETE文からの解析結果
pub struct DeleteStatement {
    pub table_name: String,
    pub filter: Option<FilterCondition>,
}

/// DROP TABLE文からの解析結果
pub struct DropTableStatement {
    pub table_name: String,
    pub if_exists: bool,
}

/// 解析されたSQL文
pub enum ParsedStatement {
    CreateTable(CreateTableStatement),
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    DropTable(DropTableStatement),
}

impl SqlParser {
    /// 新しいSQLパーサーを作成する
    pub fn new() -> Self {
        Self {
            dialect: GenericDialect {},
        }
    }
    
    /// SQL文を解析する
    pub fn parse(&self, sql: &str) -> Result<Vec<ParsedStatement>, ParseError> {
        let statements = Parser::parse_sql(&self.dialect, sql)?;
        
        let mut parsed_statements = Vec::new();
        for stmt in statements {
            let parsed = self.parse_statement(stmt)?;
            parsed_statements.push(parsed);
        }
        
        Ok(parsed_statements)
    }
    
    /// 単一のSQL文を解析する
    fn parse_statement(&self, stmt: Statement) -> Result<ParsedStatement, ParseError> {
        match stmt {
            Statement::CreateTable { name, columns, if_not_exists, .. } => {
                self.parse_create_table(name, columns, if_not_exists)
            },
            Statement::Query(query) => {
                self.parse_select(*query)
            },
            Statement::Insert { table_name, columns, source, .. } => {
                // source が Query 型の場合の対応
                if let SetExpr::Values(values) = source.body.as_ref() {
                    self.parse_insert(table_name, columns, values.clone())
                } else {
                    Err(ParseError::UnsupportedFeature("Only VALUES in INSERT are supported".to_string()))
                }
            },
            Statement::Update { table, assignments, selection, .. } => {
                self.parse_update(table, assignments, selection)
            },
            Statement::Delete { from, selection, .. } => {
                if from.len() != 1 {
                    return Err(ParseError::UnsupportedFeature("Multiple table delete not supported".to_string()));
                }
                let table_name = self.get_table_name(&from[0])?;
                self.parse_delete(table_name, selection)
            },
            Statement::Drop { object_type, names, if_exists, .. } => {
                // object_type が &str ではなく enum なのでマッチング方法を変更
                if object_type != sqlparser::ast::ObjectType::Table {
                    return Err(ParseError::UnsupportedFeature("Only DROP TABLE is supported".to_string()));
                }
                
                if names.len() != 1 {
                    return Err(ParseError::UnsupportedFeature("Multiple table drop not supported".to_string()));
                }
                Ok(ParsedStatement::DropTable(DropTableStatement {
                    table_name: self.object_name_to_string(&names[0])?,
                    if_exists,
                }))
            },
            _ => Err(ParseError::UnsupportedFeature("Unsupported SQL statement type".to_string()))
        }
    }
    
    /// CREATE TABLE文を解析する
    fn parse_create_table(
        &self, 
        name: ObjectName, 
        columns: Vec<sqlparser::ast::ColumnDef>,
        if_not_exists: bool
    ) -> Result<ParsedStatement, ParseError> {
        let table_name = self.object_name_to_string(&name)?;
        
        let mut parsed_columns = Vec::new();
        for col in columns {
            let column_name = col.name.value.clone();
            let data_type = self.parse_data_type(&col.data_type)?;
            
            let mut column = Column::new(column_name, data_type);
            
            // 制約の解析
            for constraint in &col.options {
                match constraint.option {
                    sqlparser::ast::ColumnOption::NotNull => {
                        column = column.not_null();
                    },
                    sqlparser::ast::ColumnOption::Unique { is_primary } => {
                        if is_primary {
                            column = column.primary_key();
                        } else {
                            column = column.unique();
                        }
                    },
                    sqlparser::ast::ColumnOption::Default(ref expr) => {
                        if let Expr::Value(ref val) = expr {
                            let default_value = self.sql_value_to_string(val)?;
                            column = column.with_default(default_value);
                        } else {
                            return Err(ParseError::UnsupportedFeature(
                                "Complex default expressions not supported".to_string()));
                        }
                    },
                    _ => {
                        // その他の制約は現時点ではサポートしない
                    }
                }
            }
            
            parsed_columns.push(column);
        }
        
        Ok(ParsedStatement::CreateTable(CreateTableStatement {
            table_name,
            columns: parsed_columns,
            if_not_exists,
        }))
    }
    
    /// SELECT文を解析する
    fn parse_select(&self, query: Query) -> Result<ParsedStatement, ParseError> {
        if let SetExpr::Select(select) = *query.body {
            if select.from.len() != 1 {
                return Err(ParseError::UnsupportedFeature("Joins are not supported yet".to_string()));
            }
            
            let table_name = self.get_table_name(&select.from[0])?;
            
            // カラムリストの解析
            let columns = if select.projection.iter().any(|item| matches!(item, SelectItem::Wildcard(_))) {
                None // '*' を使用した場合は全カラムを意味する
            } else {
                let mut col_names = Vec::new();
                for item in &select.projection {
                    if let SelectItem::UnnamedExpr(Expr::Identifier(ident)) = item {
                        col_names.push(ident.value.clone());
                    } else if let SelectItem::ExprWithAlias { expr: Expr::Identifier(ident), .. } = item {
                        col_names.push(ident.value.clone());
                    } else {
                        return Err(ParseError::UnsupportedFeature(
                            "Complex SELECT expressions not supported".to_string()));
                    }
                }
                Some(col_names)
            };
            
            // WHERE句の解析
            let filter = match select.selection {
                Some(expr) => Some(self.parse_filter_expression(&expr)?),
                None => None,
            };
            
            // LIMIT句の解析
            let limit = query.limit.and_then(|expr| {
                if let Expr::Value(SqlValue::Number(n, _)) = expr {
                    n.parse::<usize>().ok()
                } else {
                    None
                }
            });
            
            Ok(ParsedStatement::Select(SelectStatement {
                table_name,
                columns,
                filter,
                limit,
            }))
        } else {
            Err(ParseError::UnsupportedFeature("Only simple SELECT queries are supported".to_string()))
        }
    }
    
    /// INSERT文を解析する
    fn parse_insert(
        &self,
        table_name: ObjectName,
        columns: Vec<Ident>,
        values: Values
    ) -> Result<ParsedStatement, ParseError> {
        let table = self.object_name_to_string(&table_name)?;
        let column_names: Vec<String> = columns.into_iter().map(|ident| ident.value).collect();
        
        let mut parsed_values = Vec::new();
        for row in values.rows {
            let mut row_values = Vec::new();
            for expr in row {
                if let Expr::Value(val) = expr {
                    let value = self.sql_value_to_value(&val)?;
                    row_values.push(value);
                } else {
                    return Err(ParseError::UnsupportedFeature(
                        "Complex INSERT expressions not supported".to_string()));
                }
            }
            parsed_values.push(row_values);
        }
        
        Ok(ParsedStatement::Insert(InsertStatement {
            table_name: table,
            columns: column_names,
            values: parsed_values,
        }))
    }
    
    /// UPDATE文を解析する
/// UPDATE文を解析する
fn parse_update(
    &self,
    table: TableWithJoins,
    assignments: Vec<sqlparser::ast::Assignment>,
    selection: Option<Expr>
) -> Result<ParsedStatement, ParseError> {
    let table_name = self.get_table_name(&table)?;
    
    let mut updates = Vec::new();
    for assignment in assignments {
        // assignment.idはVec<Ident>型なので、最初の要素を使用する
        // 複合的なカラム識別子はサポートしない
        if assignment.id.len() != 1 {
            return Err(ParseError::UnsupportedFeature("Compound column identifiers not supported".to_string()));
        }
        
        let column_name = assignment.id[0].value.clone();
        
        let value = if let Expr::Value(val) = &assignment.value {
            self.sql_value_to_value(val)?
        } else {
            return Err(ParseError::UnsupportedFeature("Complex UPDATE expressions not supported".to_string()));
        };
        
        updates.push((column_name, value));
    }
    
    let filter = match selection {
        Some(expr) => Some(self.parse_filter_expression(&expr)?),
        None => None,
    };
    
    Ok(ParsedStatement::Update(UpdateStatement {
        table_name,
        updates,
        filter,
    }))
}
    
    /// DELETE文を解析する
    fn parse_delete(
        &self,
        table_name: String,
        selection: Option<Expr>
    ) -> Result<ParsedStatement, ParseError> {
        let filter = match selection {
            Some(expr) => Some(self.parse_filter_expression(&expr)?),
            None => None,
        };
        
        Ok(ParsedStatement::Delete(DeleteStatement {
            table_name,
            filter,
        }))
    }
    
    /// ObjectNameを文字列に変換する
    fn object_name_to_string(&self, name: &ObjectName) -> Result<String, ParseError> {
        if name.0.len() != 1 {
            return Err(ParseError::UnsupportedFeature("Schema qualified names not supported".to_string()));
        }
        Ok(name.0[0].value.clone())
    }
    
    /// テーブル名を取得する
    fn get_table_name(&self, table: &TableWithJoins) -> Result<String, ParseError> {
        if let TableFactor::Table { name, .. } = &table.relation {
            self.object_name_to_string(name)
        } else {
            Err(ParseError::UnsupportedFeature("Complex table sources not supported".to_string()))
        }
    }
    
    /// SQL文のデータ型をドメインデータ型に変換する
    fn parse_data_type(&self, data_type: &sqlparser::ast::DataType) -> Result<DataType, ParseError> {
        match data_type {
            sqlparser::ast::DataType::Int(_) | 
            sqlparser::ast::DataType::Integer(_) |
            sqlparser::ast::DataType::BigInt(_) => Ok(DataType::Integer),
            
            sqlparser::ast::DataType::Float(_) |
            sqlparser::ast::DataType::Double |
            sqlparser::ast::DataType::Real => Ok(DataType::Float),
            
            sqlparser::ast::DataType::Char(_) |
            sqlparser::ast::DataType::Varchar(_) |
            sqlparser::ast::DataType::Text => Ok(DataType::Text),
            
            sqlparser::ast::DataType::Boolean => Ok(DataType::Boolean),
            
            sqlparser::ast::DataType::Timestamp(_, _) |  // 2つの引数を持つバージョン
            sqlparser::ast::DataType::Date => Ok(DataType::Timestamp),
            
            _ => Err(ParseError::InvalidDataType(format!("Unsupported data type: {:?}", data_type)))
        }
    }
    
    /// SQL値をドメイン値に変換する
    fn sql_value_to_value(&self, value: &SqlValue) -> Result<Value, ParseError> {
        match value {
            SqlValue::Number(n, _) => {
                if n.contains('.') {
                    match n.parse::<f64>() {
                        Ok(f) => Ok(Value::Float(f)),
                        Err(_) => Err(ParseError::InvalidValue(format!("Invalid float value: {}", n)))
                    }
                } else {
                    match n.parse::<i64>() {
                        Ok(i) => Ok(Value::Integer(i)),
                        Err(_) => Err(ParseError::InvalidValue(format!("Invalid integer value: {}", n)))
                    }
                }
            },
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(Value::Text(s.clone()))
            },
            SqlValue::Boolean(b) => Ok(Value::Boolean(*b)),
            SqlValue::Null => Ok(Value::Null),
            _ => Err(ParseError::InvalidValue(format!("Unsupported value type: {:?}", value)))
        }
    }
    
    /// SQL値を文字列表現に変換する
    fn sql_value_to_string(&self, value: &SqlValue) -> Result<String, ParseError> {
        match value {
            SqlValue::Number(n, _) => Ok(n.clone()),
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => Ok(s.clone()),
            SqlValue::Boolean(b) => Ok(b.to_string()),
            SqlValue::Null => Ok("NULL".to_string()),
            _ => Err(ParseError::InvalidValue(format!("Unsupported value type: {:?}", value)))
        }
    }
    
    /// WHERE句の式をフィルター条件に変換する
    /// WHERE句の式をフィルター条件に変換する
fn parse_filter_expression(&self, expr: &Expr) -> Result<FilterCondition, ParseError> {
    match expr {
        // AND条件
        Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::And, right } => {
            let left_condition = self.parse_filter_expression(left)?;
            let right_condition = self.parse_filter_expression(right)?;
            
            // 既にAND条件ならマージする
            match (left_condition, right_condition) {
                (FilterCondition::And(mut left_conds), FilterCondition::And(right_conds)) => {
                    left_conds.extend(right_conds);
                    Ok(FilterCondition::And(left_conds))
                },
                (FilterCondition::And(mut conds), right_cond) => {
                    conds.push(right_cond);
                    Ok(FilterCondition::And(conds))
                },
                (left_cond, FilterCondition::And(mut conds)) => {
                    conds.insert(0, left_cond);
                    Ok(FilterCondition::And(conds))
                },
                (left_cond, right_cond) => {
                    Ok(FilterCondition::And(vec![left_cond, right_cond]))
                }
            }
        },
        
        // OR条件
        Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::Or, right } => {
            let left_condition = self.parse_filter_expression(left)?;
            let right_condition = self.parse_filter_expression(right)?;
            
            // 既にOR条件ならマージする
            match (left_condition, right_condition) {
                (FilterCondition::Or(mut left_conds), FilterCondition::Or(right_conds)) => {
                    left_conds.extend(right_conds);
                    Ok(FilterCondition::Or(left_conds))
                },
                (FilterCondition::Or(mut conds), right_cond) => {
                    conds.push(right_cond);
                    Ok(FilterCondition::Or(conds))
                },
                (left_cond, FilterCondition::Or(mut conds)) => {
                    conds.insert(0, left_cond);
                    Ok(FilterCondition::Or(conds))
                },
                (left_cond, right_cond) => {
                    Ok(FilterCondition::Or(vec![left_cond, right_cond]))
                }
            }
        },
        
        // 単純な比較演算
        Expr::BinaryOp { left, op, right } => {
            if let (Expr::Identifier(ident), Expr::Value(value)) = (&**left, &**right) {
                let column = ident.value.clone();
                let operator = match op {
                    sqlparser::ast::BinaryOperator::Eq => FilterOperator::Equal,
                    sqlparser::ast::BinaryOperator::NotEq => FilterOperator::NotEqual,
                    sqlparser::ast::BinaryOperator::Gt => FilterOperator::Greater,
                    sqlparser::ast::BinaryOperator::GtEq => FilterOperator::GreaterOrEqual,
                    sqlparser::ast::BinaryOperator::Lt => FilterOperator::Less,
                    sqlparser::ast::BinaryOperator::LtEq => FilterOperator::LessOrEqual,
                    // Like演算子がBinaryOperatorにない場合は別の方法で処理する必要があります
                    // 現在のバージョンではサポートしていないようです
                    _ => return Err(ParseError::UnsupportedFeature(
                        format!("Unsupported operator: {:?}", op)))
                };
                let value = self.sql_value_to_value(value)?;
                
                Ok(FilterCondition::Simple {
                    column,
                    operator,
                    value,
                })
            } else if let (Expr::Value(value), Expr::Identifier(ident)) = (&**left, &**right) {
                // 左右が逆の場合も対応
                let column = ident.value.clone();
                let operator = match op {
                    sqlparser::ast::BinaryOperator::Eq => FilterOperator::Equal,
                    sqlparser::ast::BinaryOperator::NotEq => FilterOperator::NotEqual,
                    sqlparser::ast::BinaryOperator::Lt => FilterOperator::Greater, // 左右反転
                    sqlparser::ast::BinaryOperator::LtEq => FilterOperator::GreaterOrEqual, // 左右反転
                    sqlparser::ast::BinaryOperator::Gt => FilterOperator::Less, // 左右反転
                    sqlparser::ast::BinaryOperator::GtEq => FilterOperator::LessOrEqual, // 左右反転
                    _ => return Err(ParseError::UnsupportedFeature(
                        format!("Unsupported operator: {:?}", op)))
                };
                let value = self.sql_value_to_value(value)?;
                
                Ok(FilterCondition::Simple {
                    column,
                    operator,
                    value,
                })
            } else {
                Err(ParseError::UnsupportedFeature("Complex conditions not supported".to_string()))
            }
        },
        
        // その他の式はサポートしない
        _ => Err(ParseError::UnsupportedFeature("Unsupported WHERE expression".to_string()))
    }
}
}

impl Default for SqlParser {
    fn default() -> Self {
        Self::new()
    }
}