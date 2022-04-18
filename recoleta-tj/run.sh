 
#!/bin/bash

####################################################################
#
# Autor      : Dadosjusbr <dadosjusbr@gmail.com>
# Site       : https://dadosjusbr.org/
# Licença    : MIT
# Descrição  : Recoleta dados dos TJs
# Projeto    : https://github.com/dadosjusbr/scripts/recoleta-tj
#
####################################################################


# Pega o nome de todos os órgãos, meses e anos, passados nos arquivos .txt
aids="${aids:=$(cat ./aids.txt)}"
years="${years:=$(cat ./years.txt)}"
months="${months:=$(cat ./months.txt)}"

for aid in ${aids[@]}; do
    for year in ${years[@]}; do
        for month in ${months[@]}; do
            go run main.go --aid=$aid --year=$year --month=$month
        done
    done
done