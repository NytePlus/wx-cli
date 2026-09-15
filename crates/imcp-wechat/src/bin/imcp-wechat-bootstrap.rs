//! Explicit, user-run Terminal bootstrap. No key files or key CLI arguments.
use anyhow::{bail,Context,Result};
use std::{io::{Read,Write},os::unix::net::UnixStream,time::Duration};
#[tokio::main]
async fn main() {
    if let Err(e)=run().await {eprintln!("密钥提取未完成：{e}");std::process::exit(1);}
}
async fn run()->Result<()> {
    let args:Vec<_>=std::env::args().collect();
    if args.len()!=4 {bail!("请使用 iMCP 设置向导生成的命令。");}
    if !wx_keychain::check_sip().passed {bail!("当前 SIP 已启用。首次捕获需按设置向导临时关闭 SIP；不会自动修改系统设置。");}
    let mut stream=UnixStream::connect(&args[1]).context("设置窗口已关闭或命令已过期")?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let handshake=serde_json::to_vec(&serde_json::json!({"token":args[2]}))?;
    stream.write_all(&(handshake.len() as u32).to_be_bytes())?;stream.write_all(&handshake)?;
    let mut ack=[0];stream.read_exact(&mut ack)?;if ack[0]!=1 {bail!("授权令牌无效");}
    let accounts=wx_keychain::find_account_dirs()?;
    let selected:Vec<_>=accounts.into_iter().filter(|a|a.account_id==args[3]).collect();
    if selected.is_empty() {bail!("所选微信账号未找到");}
    println!("将重启微信并等待登录，以捕获此账号的数据库密钥。完成后请恢复 SIP。");
    let result=wx_keychain::capture_key(&selected,Duration::from_secs(120)).await.map_err(|_|anyhow::anyhow!("未获得有效密钥；检查微信版本、登录状态和调试权限"))?;
    let payload=serde_json::to_vec(&serde_json::json!({"account":result.matched_account.account_id,"key":hex::encode(result.raw_key)}))?;
    stream.write_all(&(payload.len() as u32).to_be_bytes())?;stream.write_all(&payload)?;
    stream.read_exact(&mut ack)?;if ack[0]!=1 {bail!("iMCP 未确认保存密钥");}
    println!("密钥已交给 iMCP Keychain。现在可以恢复 SIP。");Ok(())
}
