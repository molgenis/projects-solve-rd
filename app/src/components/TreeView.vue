
<template>
  <div class="tree__container">
    <h3 :id="`${id}-title`" class="tree__title" v-if="title">{{ title }}</h3>
    <ul :id="id" :aria-labelledby="`${id}-title`" class="tree" role="tree">
      <li class="tree__item" v-for="row in data" :key="row.id">
        <TreeViewItem
          :id="row.id"
          :name="row.name"
          :children="row.children ? row.children : null"
        />
      </li>
    </ul>
  </div>
</template>

<script>
import TreeViewItem from './TreeVueItem.vue'

export default {
  name: 'tree-view',
  props: {
    id: {
      type: String,
      required: true
    },
    title: {
      type: String,
      required: false
    },
    data: {
      type: Array,
      required: true
    }
  },
  components: {
    TreeViewItem
  }
}
</script>

<style lang="scss" scoped>
.tree__container {
  .tree__title {
    margin: 0;
  }
  .tree {
    box-sizing: border-box;
    padding: 1.5em;
    border: 1px solid #c4c4c4;
    list-style: none;
    margin: 0;
    .tree__item {
      position: relative;
      margin-left: 6px;
      margin-bottom: 8px;
      font-size: 11pt;
    }
  }
}
</style>
