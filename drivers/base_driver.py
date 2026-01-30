from playwright.async_api import async_playwright

class BaseScraper:
    def __init__(self):
        self.browser = None
        self.context = None

    async def init_browser(self, headless=False):
        self.playwright = await async_playwright().start()

        executable = r"C:\Users\mwhiting\Downloads\chrome-win\chrome-win\chrome.exe"
        # self.browser = await self.playwright.chromium.launch(headless=headless)
        # # Contexto con User-Agent para parecer un humano
        # self.context = await self.browser.new_context(
        #     user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        # )

        try:
            self.browser = await self.playwright.chromium.launch(
                executable_path=executable,
                headless=headless
            )
            self.context = await self.browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        except Exception as e:
            print(f"❌ Error al iniciar el navegador: {e}")
            print(f"Asegúrate de que la ruta '{executable}' sea correcta.")
            raise e

    async def close_browser(self):
        if self.browser:
            await self.browser.close()
            await self.playwright.stop()

    async def get_safe_text(self, element, selector):
        """Helper para extraer texto sin que el script explote si no encuentra el selector"""
        try:
            target = await element.query_selector(selector)
            if target:
                text = await target.inner_text()
                return text.strip()
            return None
        except Exception:
            return None