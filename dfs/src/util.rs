use anyhow::Result;

pub fn flatten_result<T, E1, E2>(val: Result<Result<T, E1>, E2>) -> Result<T>
where
    E1: Into<anyhow::Error>,
    E2: Into<anyhow::Error>,
{
    match val {
        Ok(Ok(r)) => Ok(r),
        Ok(Err(e)) => Err(e.into()),
        Err(e) => Err(e.into()),
    }
}
