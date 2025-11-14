SELECT
  a.id_municipio,
  a.cor_raca,
  a.sexo,
  a.grupo_idade,
  a.alfabetizacao,

  -- populaçao vinda da outra tabela
  CAST(p.populacao AS INT64) AS populacao_total_grupo

FROM `basedosdados.br_ibge_censo_2022.alfabetizacao_grupo_idade_sexo_raca` a
LEFT JOIN `basedosdados.br_ibge_censo_2022.populacao_idade_sexo` p
  ON a.id_municipio = p.id_municipio;