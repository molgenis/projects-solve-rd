
<template>
  <div class="tree__container">
    <h3 :id="`${id}-title`" class="tree__title" v-if="title">{{ title }}</h3>
    <ul :id="id" :aria-labelledby="`${id}-title`" class="tree" role="tree">
      <li class="tree__item" v-for="row in data" :key="row.id">
        <TreeViewItem
          :id="row.id"
          :name="row.subjectID"
          :group="JSON.parse(row.json).group"
          :children="JSON.parse(row.json).children"
        />
      </li>
    </ul>
  </div>
</template>

<script>
import TreeViewItem from './TreeViewItem.vue'

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
    border-radius: 6px;
    padding: 0;
    margin: 0;
    list-style: none;
    .tree__item {
      font-size: 11pt;
    }
  }
}
</style>
