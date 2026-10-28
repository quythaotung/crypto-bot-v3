    async with SCAN_LOCK:
        SYSTEM["scanning"]=True
        SYSTEM["started"]=utc().isoformat()
        ok=bad=err=events=0
        try:
            symbols=await asyncio.to_thread(scanner.refresh_symbols)
            SYSTEM["discover_error"]=None
            SYSTEM["total"]=len(symbols)
            SYSTEM["excluded"]=scanner.excluded
            btc=None
            try:
                btc_df=TechnicalAnalyzer.add_indicators(await asyncio.to_thread(cache.get,"BTCUSDT","60",75))
                if len(btc_df)>=25:
                    btc=(float(btc_df["close"].iloc[-1])/float(btc_df["close"].iloc[-25])-1)*100
            except Exception as exc:
                log.warning("No BTC comparison data: %s",exc)
            loop=asyncio.get_running_loop()
            with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                for offset in range(0,len(symbols),BATCH):
                    chunk=symbols[offset:offset+BATCH]
                    futures=[loop.run_in_executor(pool,scanner.scan_symbol,s,btc) for s in chunk]
                    for result in await asyncio.gather(*futures,return_exceptions=True):
                        if isinstance(result,BaseException):
                            err+=1
                            log.error("Scan symbol error: %s",result)
                        else:
                            status,n=result
                            if status=="ok": ok+=1
                            else: bad+=1
                            events+=n
                    # If Telegram is unavailable, retain notifications in SQLite.
                    await send_outbox(app)
            db.scan_log(len(symbols),ok,bad,err,events,scanner.excluded)
        except Exception as exc:
            SYSTEM["discover_error"]=str(exc)
            log.exception("Market scan failed")
        finally:
            SYSTEM.update({"scanning":False,"finished":utc().isoformat(),
                           "ok":ok,"nodata":bad,"errors":err,"signals":events})
            log.info("scan complete: ok=%s nodata=%s errors=%s events=%s",ok,bad,err,events)
            await send_outbox(app)


async def cmd_scan(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if not await allowed(update): return
    if SCAN_LOCK.locked():
        await update.message.reply_text("An automatic scan is already running.")
        return
    await update.message.reply_text("Starting diagnostic scan; the regular schedule remains enabled.")
    context.application.create_task(do_scan(context.application))


def next_scan_time(now:datetime)->datetime:
    candidate=now.replace(minute=0,second=0,microsecond=0)+timedelta(seconds=SCAN_DELAY_SECONDS)
    if candidate<=now:
        candidate+=timedelta(hours=1)
    return candidate


async def auto_scheduler(app:Application):
    # Starts immediately, then aligns to the exact next H1 close + 90 seconds.
    while True:
        nxt=next_scan_time(utc())
        SYSTEM["next"]=nxt.isoformat()
        await asyncio.sleep(max(0.1,(nxt-utc()).total_seconds()))
        await do_scan(app)


async def on_post_init(app:Application):
    # Unmanaged asyncio tasks are explicitly cancelled in post_shutdown;
    # Application.create_task is awaited by Application.stop and would block
    # shutdown forever for an infinite scheduler.
    app.bot_data["worker_tasks"]=[
        asyncio.create_task(auto_scheduler(app),name="hourly-market-observer"),
        asyncio.create_task(do_scan(app),name="first-market-scan"),
    ]


async def on_post_shutdown(app:Application):
    tasks=app.bot_data.get("worker_tasks",[])
    for t in tasks:
        t.cancel()
    if tasks:
        await asyncio.gather(*tasks,return_exceptions=True)


def main():
    if not BOT_TOKEN or not CHAT_ID or not ALLOWED_CHAT_IDS:
        raise SystemExit("Set BOT_TOKEN and CHAT_ID (and optionally ALLOWED_CHAT_IDS) in hosting env")
    try:
        int(CHAT_ID)
    except ValueError:
        raise SystemExit("CHAT_ID must be numeric")
    threading.Thread(target=serve_health,daemon=True).start()
    app=ApplicationBuilder().token(BOT_TOKEN).post_init(on_post_init).post_shutdown(on_post_shutdown).build()
    for name,fn in (("start",cmd_start),("status",cmd_status),("active",cmd_active),
                    ("watch",cmd_watch),("stats",cmd_stats),("help",cmd_help),("scan",cmd_scan)):
        app.add_handler(CommandHandler(name,fn))
    log.info("Starting paper-only auto observer (no order functionality)")
    app.run_polling(drop_pending_updates=True)


if __name__=="__main__":
    main()
