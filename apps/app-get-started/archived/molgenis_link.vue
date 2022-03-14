<template>
    <a :href="href">{{ label }}</a>
</template>

<script>
export default {
  props: {
    host: String,
    entity: String,
    label: String,
    columns: {
      type: Array,
      default: () => []
    },
    group: {
      type: String,
      default: () => ''
    },
    patch: {
      type: String,
      default: () => ''
    },
    mod: {
      type: String,
      default: () => 'data'
    }
  },
  computed: {
    href () {
      let url =
                this.host +
                '/menu/main/dataexplorer?' +
                'entity=' +
                this.entity +
                '&mod=' +
                this.mod

      // if columns, make sure each attribute is prefixed with `[]`
      if (
        typeof this.columns !== 'undefined' ||
                this.columns.length !== 0
      ) {
        const attrs = this.columns.map(
          (attr) => '&attrs%5B%5D=' + attr
        )
        url = url + attrs.join('')
      }

      if (this.patch !== '') url = url + '&filter=patch==' + this.patch
      if (this.group !== '') url = url + '&filter=ERN==' + this.group

      return url + '&hideselected=true'
    }
  }
}
</script>
