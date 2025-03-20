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


    Ok(())

}
