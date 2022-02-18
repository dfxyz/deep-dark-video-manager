use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{Attribute, ItemStruct};

#[proc_macro_attribute]
pub fn table_name(_: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_derive(CreateTable, attributes(primary))]
pub fn derive_create_table(stream: TokenStream) -> TokenStream {
    let input: ItemStruct = syn::parse(stream).unwrap();
    let table_name = determine_table_name(&input.attrs);

    let mut sql = String::from("CREATE TABLE IF NOT EXISTS ");
    sql.push_str(&table_name);
    sql.push('(');
    for field in &input.fields {
        let column_name = field.ident.as_ref().unwrap().to_token_stream().to_string();
        sql.push_str(&column_name);

        let ty = field.ty.to_token_stream().to_string();
        if ty == "String" {
            sql.push_str(" TEXT NOT NULL");
        } else {
            sql.push_str(" INTEGER NOT NULL");
        }
        for attr in &field.attrs {
            let attr = attr.path.to_token_stream().to_string();
            if attr == "primary" {
                sql.push_str(" PRIMARY KEY");
                break;
            }
        }
        sql.push_str(", ");
    }
    sql.pop();
    sql.pop();
    sql.push(')');

    let struct_name = &input.ident;
    let output = quote! {
        impl #struct_name {
            #[inline]
            fn create_table(db: &::rusqlite::Connection) -> ::rusqlite::Result<()> {
                db.execute(#sql, [])?;
                Ok(())
            }
        }
    };
    TokenStream::from(output)
}

#[proc_macro_derive(CreateIndex, attributes(index))]
pub fn derive_create_index(stream: TokenStream) -> TokenStream {
    let input: ItemStruct = syn::parse(stream).unwrap();
    let table_name = determine_table_name(&input.attrs);

    let mut sqls = vec![];
    'field: for field in &input.fields {
        for attr in &field.attrs {
            let attr = attr.path.to_token_stream().to_string();
            if attr == "index" {
                let column_name = field.ident.as_ref().unwrap().to_token_stream().to_string();
                let mut sql = String::from("CREATE INDEX IF NOT EXISTS idx_");
                sql.push_str(&table_name);
                sql.push('_');
                sql.push_str(&column_name);
                sql.push_str(" ON ");
                sql.push_str(&table_name);
                sql.push('(');
                sql.push_str(&column_name);
                sql.push(')');
                sqls.push(sql);
                continue 'field;
            }
        }
    }

    let struct_name = &input.ident;
    let output = quote! {
        impl #struct_name {
            #[inline]
            fn create_indexes(db: &::rusqlite::Connection) -> ::rusqlite::Result<()> {
                #(
                    db.execute(#sqls, [])?;
                )*
                Ok(())
            }
        }
    };
    TokenStream::from(output)
}

#[proc_macro_derive(Select)]
pub fn derive_select(stream: TokenStream) -> TokenStream {
    let input: ItemStruct = syn::parse(stream).unwrap();
    let table_name = determine_table_name(&input.attrs);

    let mut fields = vec![];
    let mut sql = String::from("SELECT ");
    for field in input.fields {
        let field = field.ident.as_ref().unwrap().to_token_stream();
        let column_name = field.to_string();
        fields.push(field);
        sql.push_str(&column_name);
        sql.push_str(", ");
    }
    sql.pop();
    sql.pop();
    sql.push_str(" FROM ");
    sql.push_str(&table_name);

    let struct_name = &input.ident;
    let output = quote! {
        impl #struct_name {
            fn select<P: ::rusqlite::Params>(db: &::rusqlite::Connection, where_expr: &str, where_params: P) -> ::rusqlite::Result<Vec<Self>> {
                let mut stmt = String::from(#sql);
                if where_expr.len() > 0 {
                    stmt.push(' ');
                    stmt.push_str(where_expr);
                }
                let mut stmt = db.prepare(&stmt)?;
                let mut rows = stmt.query_map(where_params, |row| {
                    let mut entry = #struct_name::default();
                    let mut i = 0;
                    #(
                        entry.#fields = row.get(i)?;
                        i += 1;
                    )*
                    Ok(entry)
                })?;
                let mut result = vec![];
                for row in rows {
                    result.push(row?);
                }
                Ok(result)
            }
        }
    };
    TokenStream::from(output)
}

#[proc_macro_derive(Insert)]
pub fn derive_insert(stream: TokenStream) -> TokenStream {
    let input: ItemStruct = syn::parse(stream).unwrap();
    let table_name = determine_table_name(&input.attrs);

    let mut fields = vec![];
    let mut sql = String::from("INSERT INTO ");
    sql.push_str(&table_name);
    sql.push('(');
    for field in input.fields {
        let field = field.ident.as_ref().unwrap().to_token_stream();
        let column_name = field.to_string();
        fields.push(field);
        sql.push_str(&column_name);
        sql.push_str(", ");
    }
    sql.pop();
    sql.pop();
    sql.push_str(") VALUES (");
    for _ in 0..fields.len() {
        sql.push_str("?,");
    }
    sql.pop();
    sql.push(')');

    let struct_name = &input.ident;
    let output = quote! {
        impl #struct_name {
            fn insert(&self, db: &::rusqlite::Connection) -> ::rusqlite::Result<()> {
                let mut stmt = db.prepare(#sql)?;
                let mut params = ::rusqlite::params![#(self.#fields),*];
                stmt.execute(params)?;
                Ok(())
            }
        }
    };
    TokenStream::from(output)
}

fn determine_table_name(attrs: &[Attribute]) -> String {
    for attr in attrs {
        if attr.path.to_token_stream().to_string() == "table_name" {
            let name = attr.tokens.to_string();
            if name.is_empty() {
                panic!("attribute 'table_name' has no value");
            }
            let name = name[1..name.len() - 1].to_string();
            if name.is_empty() {
                panic!("attribute 'table_name' has no value");
            }
            return name;
        }
    }
    panic!("no attribute 'table_name'");
}
