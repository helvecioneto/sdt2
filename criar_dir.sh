#!/bin/bash

# Lista de diretórios a serem criados
dirs=(
    "./input/dados_temporarios/"
    "./input/sky_camera/"
    "./input/preprocessing/"
    "./input/operational/"
    "./output/dados_formatados/"
    "./output/temporary_data/"
    "./output/dados_historicos/"
    "./output/debug/"
    "/restricted/coleta/"
    "/restricted/dados/sonda/historico/"
    "./output/dados_qualificados/"
    "./output/dados_web/"
)

# Criar os diretórios, se não existirem
for dir in "${dirs[@]}"; do
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
        echo "Diretório criado: $dir"
    else
        echo "Diretório já existe: $dir"
    fi
done

echo "Todos os diretórios foram processados."
