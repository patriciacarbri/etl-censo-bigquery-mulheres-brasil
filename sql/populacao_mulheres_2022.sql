SELECT
  t.id_municipio,
  m.nome AS nome_municipio,
  m.sigla_uf,
  t.sexo,
  t.cor_raca,
  t.grupo_idade,
  t.alfabetizacao,
  COALESCE(CAST(t.populacao AS INT64), 0) AS populacao
FROM `basedosdados.br_ibge_censo_2022.alfabetizacao_grupo_idade_sexo_raca` AS t
LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` AS m
  ON t.id_municipio = m.id_municipio
ORDER BY
  t.id_municipio,
  t.sexo,
  t.cor_raca,
  t.grupo_idade,
  t.alfabetizacao;
