 
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

for aid in ${aids[@]}; do
    go run main.go --aid=$aid
done