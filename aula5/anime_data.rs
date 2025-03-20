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

pub fn mesclando_datasets(df: &DataFrame) -> Result<DataFrame, PolarsError> {
    let animes =df.clone().lazy();

    let file_path = "C:\\Users\\Allan\\OneDrive - Fiap-Faculdade de Informática e Administração Paulista\\Cursos\\Polars-Rust\\projeto_polars\\src\\dados\\animelist2.csv";
    let file = File::open(file_path)?;
    
    // Usando a API correta para CsvReader
    let ratings = CsvReader::new(file)
        .finish()?;

    
    // Inner join dos datasets
    let joined = animes.lazy()
        .join(
            ratings.lazy(),
            [col("ID")],
            [col("anime_id")],
            JoinArgs::default(),
        )
        .collect()?;
    
    // Retorna apenas as 5 primeiras linhas
    //let result = joined.head(Some(5));
        
    Ok(joined)
}


pub fn processamento_texto(df: &DataFrame) -> Result<DataFrame, PolarsError> {

    let df = df.clone().lazy();
    
    // Limpar e tokenizar sinopses
    
    let palavras = df.lazy()
            .select([col("ID"), col("Titulo"), col("sypnopsis")])
            .filter(col("sypnopsis").is_not_null())
            .with_column(
                col("sypnopsis")
                .str().to_lowercase()
                .str().replace_all(lit("[^a-z\\s]"), lit(""), true)
                .alias("texto_limpo"))
            .with_column(
                col("texto_limpo")
                    .str().split(lit(" ")) // Tokeniza o texto em palavras
                    .alias("palavras_tokenizadas")
            )
            .explode(["palavras_tokenizadas"]) // Expande cada palavra em uma linha separada
            .with_column(
                col("palavras_tokenizadas")
                .str().replace_all(lit("^$"), lit(""), true) // Substitui strings vazias por null
                .alias("palavras_filtradas")
            )
            .filter(col("palavras_filtradas").is_not_null()) // Remove strings vazias
            .collect()?;
    
    // Contar frequência das principais palavras
    
    let freq_palavras = palavras.lazy()
                        .group_by([col("palavras_filtradas")])
                        .agg([
                            col("ID").count().alias("frequencia")
                        ])
                        .sort(
                            ["frequencia"],
                        SortMultipleOptions {
                            descending: vec![true], // Sort in descending order
                            ..SortMultipleOptions::new() // Default values for other fields
                        }
                        ).limit(5).collect()?;
    Ok(freq_palavras)
    
    }
  
pub fn trabalhando_com_listas(df: &DataFrame) -> Result<DataFrame, PolarsError> {
        let df = df.clone().lazy();
    
        let generos = df.clone().lazy()
            .select([col("ID"), col("Titulo"), col("Score"), col("Genres")])
            .with_column(
                col("Genres").str().split(lit(", "))
                    .alias("generos_lista")
            )
            .explode(["generos_lista"])
            .collect()?;
    
        let medias_por_genero = generos.lazy()
            .group_by([col("generos_lista")])
            .agg([
                col("ID").count().alias("quantidade"),
                col("Score").mean().alias("nota_media")
            ])
            .sort_by_exprs(
                [col("quantidade")],
                SortMultipleOptions {
                    descending: vec![true],
                    ..SortMultipleOptions::default()
                }
            )
            .collect()?;

        // Criar o diretório "resultados" se não existir
        std::fs::create_dir_all("resultados")
        .map_err(|e| PolarsError::ComputeError(format!("Erro ao criar diretório 'resultados': {}", e).into()))?;
        
        // Salvar o resultado em formato Parquet
        let mut parquet_file = std::fs::File::create("resultados/medias_por_genero.parquet")
            .map_err(|e| PolarsError::ComputeError(format!("Erro ao criar arquivo Parquet: {}", e).into()))?;
        
        ParquetWriter::new(&mut parquet_file)
            .finish(&mut medias_por_genero.clone())
            .map_err(|e| PolarsError::ComputeError(format!("Erro ao escrever arquivo Parquet: {}", e).into()))?;
        
        // Salvar o resultado em formato JSON
        let mut json_file = std::fs::File::create("resultados/medias_por_genero.json")
            .map_err(|e| PolarsError::ComputeError(format!("Erro ao criar arquivo JSON: {}", e).into()))?;
        
        JsonWriter::new(&mut json_file)
            .with_json_format(JsonFormat::Json)
            .finish(&mut medias_por_genero.clone())
            .map_err(|e| PolarsError::ComputeError(format!("Erro ao escrever arquivo JSON: {}", e).into()))?;
        
        Ok(medias_por_genero)
}
    

pub fn processamento_grandes_volumes() -> Result<(), PolarsError> {
        let file_path = "C:\\Users\\Allan\\OneDrive - Fiap-Faculdade de Informática e Administração Paulista\\Cursos\\Polars-Rust\\projeto_polars\\src\\dados\\animelist.csv";
        
        // Usando LazyFrame para processamento em lotes
        let lf = LazyCsvReader::new(file_path)
            .finish()?;
        
        // Computação da média de ratings usando operações lazy
        let result = lf.select([
            col("rating").mean().alias("media_rating"),
            col("rating").count().alias("total_registros")
        ]).collect()?;
        
        // Extração dos resultados
        let media_rating = result.column("media_rating")?.f64()?.get(0).unwrap_or(0.0);
        let total_registros = result.column("total_registros")?.u32()?.get(0).unwrap_or(0);
        
        println!("Total de registros: {}", total_registros);
        println!("Média de ratings: {:.2}", media_rating);
        
        Ok(())
}
