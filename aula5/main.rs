mod anime_data;
use polars::error::PolarsError;

fn main() -> Result<(), PolarsError> {
    //Result<DataFrame, PolarsError>
    // Imprime DataFrame
    //anime_data::imprimir_dataframe();

    let _df = match anime_data::le_e_filtra_notas() {
        Ok(df) => println!("{}", df),
        Err(e) => eprintln!("Erro ao ler e filtrar notas: {}", e),
    };
    let df = anime_data::le_e_filtra_notas().unwrap();
    println!("{}", df);

    let _ = anime_data::explorar_dataset(&df);
    let df_result = anime_data::operacoes_colunas(&df);
    
    // Verifica se ocorreu um erro durante as operações
    let df = match df_result {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Erro ao realizar operações nas colunas: {}", e);
            return Err(e); // Propaga o erro para main
        }
    };

    // Imprime o DataFrame resultante
    println!("{:?}", df);

    // Imprime os Top 10 de ação
    let top10 = anime_data::filtragem_avancada(&df);
    println!("{:?}", top10);

    //Agregações básicas
    let contagem_por_nota = anime_data::agregacoes(&df);
    println!("{:?}", contagem_por_nota);

    //Mesclando dataset
    let joined = anime_data::mesclando_datasets(&df);
    println!("{:?}", joined);

    let df_joined = match joined {
        Ok(df_joined) => df_joined,
        Err(e) => {
            eprintln!("Erro ao realizar operações nas colunas: {}", e);
            return Err(e); // Propaga o erro para main
        }
    };

    //Processamento de texto
    let freq_palavras = anime_data::processamento_texto(&df_joined);
    println!("{:?}", freq_palavras);

    //Trabalhando com listas
    let medias_por_genero = anime_data::trabalhando_com_listas(&df_joined);
    println!("{:?}", medias_por_genero);

    //Processamento de grandes volumes de dados
    anime_data::processamento_grandes_volumes()?;


    Ok(())
    
}
