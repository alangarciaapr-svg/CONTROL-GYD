APP_NAME = "SEGAV ERP"
APP_VERSION = "8.4.66"
DB_PATH = "app.db"
UPLOAD_ROOT = "uploads"
MAX_UPLOAD_FILE_BYTES = int(1.5 * 1024 * 1024)
UPLOAD_HELP_TEXT = (
    "Máximo por archivo: 1,5 MB. Si el archivo supera ese tamaño, la app intentará comprimirlo automáticamente. "
    "Si aun así excede el límite, redúcelo antes de subirlo. Sugerencia: puedes comprimirlo en iLovePDF."
)
