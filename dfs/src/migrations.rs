use crate::DB;
use anyhow::Result;
use sqlx::query;

macro_rules! version_check {
    ($version:expr) => {{
        const VERSION: i64 = $version;
        if get_version().await? >= VERSION {
            return Ok(());
        }
        println!("running migration {}", VERSION);

        || {
            let db = DB.get().unwrap();
            sqlx::query!(
                "UPDATE meta SET value = ? WHERE key = 'schema_version'",
                VERSION
            )
            .execute(&db.pool)
        }
    }};
}

async fn get_version() -> Result<i64> {
    let db = DB.get().unwrap();

    let res = query!("select value from meta where key = 'schema_version'")
        .fetch_one(&db.pool)
        .await?
        .value
        .unwrap()
        .parse()?;
    Ok(res)
}

async fn one() -> Result<()> {
    let done = version_check!(1);

    todo!("some useful code should be put here");

    done().await?;
    Ok(())
}

pub async fn run() -> Result<()> {
    one().await?;

    Ok(())
}
