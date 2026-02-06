from playwright.async_api import async_playwright
import os

class BaseScraper:
    def __init__(self):
        self.browser = None
        self.context = None
        self.playwright = None

    async def init_browser(self, headless=True):
        """
        Inicia el navegador. 
        Headless=True es obligatorio para GitHub Actions.
        """
        self.playwright = await async_playwright().start()

        try:
            # NO usamos executable_path. Playwright usará el que instalamos
            # con el comando 'playwright install chromium' en el YAML.
            
            self.browser = await self.playwright.chromium.launch(
                headless=headless,
                args=["--disable-gpu", "--no-sandbox"] # Recomendado para servidores Linux
            )
            """
            # SOLO PARA TEST LOCAL SI FALLA LA DESCARGA
            self.browser = await self.playwright.chromium.launch(
                executable_path=r"C:\Program Files\Google\Chrome\Application\chrome.exe", # Ruta típica
                headless=True 
            )"""
            
            self.context = await self.browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            print("✅ Navegador iniciado correctamente")
            
        except Exception as e:
            print(f"❌ Error al iniciar el navegador: {e}")
            if self.playwright:
                await self.playwright.stop()
            raise e

    async def close_browser(self):
        """Cierra todo y limpia procesos"""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        print("🔒 Navegador cerrado")

    async def get_safe_text(self, element, selector):
        """Extrae texto de forma segura"""
        try:
            target = await element.query_selector(selector)
            if target:
                text = await target.inner_text()
                return text.strip()
            return None
        except Exception:
            return None