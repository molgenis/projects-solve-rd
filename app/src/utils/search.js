
const removeNullObjectKeys = function (data) {
  const filters = data
  Object.keys(filters).forEach(key => {
    if (filters[key] === null || filters[key] === '') {
      delete filters[key]
    }
  })
  return filters
}

const objectToUrlFilterArray = function (object) {
  const urlFilter = []
  Object.keys(object).forEach(key => {
    let filter = null
    let value = object[key].trim().replaceAll(', ', ',')
    if (value[value.length - 1] === ',') {
      value = value.slice(0, value.length - 1)
    }
    if (value.includes(',')) {
      filter = `${key}=in=(${value})`
    } else {
      filter = `${key}==${value}`
    }
    urlFilter.push(filter)
  })
  return urlFilter
}

const buildFilterUrl = function (array) {
  const filters = array.join(';')
  return encodeURIComponent(filters)
}

const windowReplaceUrl = function (filterUrl) {
  const baseUrl = '/menu/plugins/dataexplorer?entity=variantdb_variant&mod=data&hideselect=true'
  const url = baseUrl + '&filter=' + filterUrl
  window.open(url, '_blank')
}

module.exports = {
  removeNullObjectKeys,
  objectToUrlFilterArray,
  buildFilterUrl,
  windowReplaceUrl
}
