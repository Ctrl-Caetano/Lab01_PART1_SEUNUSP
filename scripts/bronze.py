import kagglehub
import shutil
import os

#raw
RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

#Dataset
print("Baixando dataset...")
path = kagglehub.dataset_download("bsthere/nike-global-catalogue-2026")
print(f"Dataset baixado em: {path}")

# Copia os arquivos para data/raw/
print("Copiando arquivos para data/raw/...")
for arquivo in os.listdir(path):
    origem = os.path.join(path, arquivo)
    destino = os.path.join(RAW_DIR, arquivo)
    shutil.copy2(origem, destino)
    print(f"  copiado: {arquivo}")

print(f"\nBronze concluido! {len(os.listdir(RAW_DIR))} arquivos em data/raw/")
