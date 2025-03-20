use polars::prelude::*;

use polars::io::json::{JsonWriter, JsonFormat};
use std::error::Error;
use std::fs::File;
// use polars::lazy::dsl::ExprNameNameSpace;


pub fn imprimir_dataframe() {
    // Criando o DataFrame dentro da função
    let df: DataFrame = df!(
        "nome" => ["Dragon Ball", "Naruto", "One Piece", "CDZ"],
        "score" => [9.0, 9.5, 10.0, 10.0]
    ).unwrap();

    // Imprimindo o DataFrame
    println!("{}", df);
}

pub fn le_e_filtra_notas() -> Result<DataFrame, PolarsError> {
    let file_path = "C:\\Users\\Allan\\OneDrive - Fiap-Faculdade de Informática e Administração Paulista\\Cursos\\Polars-Rust\\projeto_polars\\src\\dados\\animes_processado.csv";
    let file = File::open(file_path)?;
    
    // Usando a API correta para CsvReader
    let df = CsvReader::new(file)
        .finish()?;

    // Filtrar apenas os registros com Score > 5.00
    let filtered_df = df
        .lazy()
        .filter(col("Score").gt(5))
        .collect()?;
    

    //Name	Score	Genres	sypnopsis
    Ok(filtered_df)
}


pub fn explorar_dataset(df: &DataFrame) -> Result<(), PolarsError> {
    
    
    // Seleção de colunas específicas
    let selecionado = df.select(["Name", "Score"])?;
    println!("Primeiros 5 animes com suas notas:");
    println!("{}", selecionado.head(Some(5)));
    
    // Média das notas
    let score_column = df.column("Score")?;
    let media = score_column.as_series().map(|s| s.mean()).flatten().unwrap_or(0.0);
    println!("Média das notas de animes: {:.2}", media);
    
    Ok(())
}

pub fn operacoes_colunas(df: &DataFrame) -> Result<DataFrame, PolarsError> {
    
    // Criar uma nova coluna: Score categorizado
    let df = df.clone().lazy()
        .with_column(
            when(col("Score").gt(lit(8.0)))
                .then(lit("Excelente"))
                .when(col("Score").gt(lit(6.5)))
                .then(lit("Bom"))
                .otherwise(lit("Regular"))
                .alias("Categoria")
        )
        .collect()?;
    
    // Renomear colunas
    let df = df.lazy()
        .rename(["MAL_ID", "Name"], ["ID", "Titulo"],false)
        .collect()?;
        
    Ok(df)
}

pub fn filtragem_avancada(df: &DataFrame) -> Result<DataFrame, PolarsError> {
    let df = df.clone().lazy();

    let df_filtrado = df
            .filter(col("Genres").str().contains(lit("Action"),false))
            .select([col("Titulo"), col("Genres"),col("Score")])
            .collect()?;
        
    // Top 10 animes por nota
    let top_10 = df_filtrado.lazy()
        .sort_by_exprs(vec![col("Score")], 
        SortMultipleOptions {
            descending: vec![true], // Define a ordenação como decrescente
            ..Default::default()   // Mantém as outras opções padrão
        })
        .limit(10)
        .collect()?;
    

    Ok(top_10)
}


pub fn agregacoes(df: &DataFrame) -> Result<DataFrame, PolarsError> {
    let df = df.clone().lazy();

    let contagem_por_nota = df.lazy()
        .group_by([col("Categoria")])
        .agg([
            col("ID").count().alias("Quantidade"),
            col("Score").mean().alias("Média")
        ])
        .collect()?;

    Ok(contagem_por_nota)
}
