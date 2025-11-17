SELECT
  a.id_municipio,
  m.nome AS nome_municipio,
  m.sigla_uf,
  a.cor_raca,
  a.sexo,
  a.grupo_idade,
  a.alfabetizacao,
  CAST(p.populacao AS INT64) AS populacao_total_grupo

FROM `basedosdados.br_ibge_censo_2022.alfabetizacao_grupo_idade_sexo_raca` a

LEFT JOIN `basedosdados.br_ibge_censo_2022.populacao_idade_sexo` p
  ON a.id_municipio = p.id_municipio

LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` m
  ON a.id_municipio = m.id_municipio
;
