<template>
  <div v-if="children">
    <button
      :id="`toggle-${id}`"
      class="tree__toggle"
      @click="onclick"
      :aria-expanded="isOpen"
      :aria-controls="`subtree-${id}`"
    >
      <TreeViewItemIcon
        :iconType="
          (group === 'patient' && isOpen)
          ? 'folder-open'
          : (group === 'patient' && !isOpen)
            ? 'folder-closed'
            : group"
      />
      <span class="tree__item__label">{{ name }}</span>
    </button>
    <TreeViewItemLink
      :href="href"
      :hrefLabel="`go to ${group} ${name} in the database`"
      v-if="href"
    />
    <ul
      :id="`subtree-${id}`"
      class="tree__subtree"
      role="group"
      v-show="isOpen"
    >
      <li class="tree__item" v-for="child in children" :key="child.name">
        <tree-view-item
          :id="child.id"
          :name="child.name"
          :group="child.group"
          :href="child.href"
          :hrefLabel="`go to ${child.group} ${child.name} in the database`"
          :children="child.children"
        />
      </li>
    </ul>
  </div>
  <div v-else>
    <TreeViewItemIcon :iconType="group"/>
    <span class="tree__item__label">{{ name }}</span>
    <TreeViewItemLink
      :href="href"
      :hrefLabel="`go to ${group} ${name} in the database`"
      v-if="href"
    />
  </div>
</template>

<script>
import TreeViewItemIcon from './TreeViewItemIcon.vue'
import TreeViewItemLink from './TreeViewItemLink.vue'

export default {
  name: 'treeViewItem',
  props: {
    id: {
      type: String,
      required: true
    },
    name: {
      type: String,
      required: true
    },
    group: {
      type: String,
      required: true
    },
    href: {
      type: String,
      required: false
    },
    children: {
      type: Array,
      required: false
    }
  },
  components: {
    TreeViewItemIcon,
    TreeViewItemLink
  },
  data () {
    return {
      isOpen: false
    }
  },
  methods: {
    onclick () {
      this.isOpen = !this.isOpen
    }
  }
}
</script>

<style lang="scss" scoped>
.tree__subtree {
  list-style: none;
  padding: 0;
  margin-left: 42px;
  .tree__item {
    font-size: 11pt;
  }
}

.tree__toggle {
  background: none;
  border: none;
  padding: 0;
  margin: 0;
  font-size: inherit;
  cursor: pointer;
  
  .tree__item__label {
    text-decoration: underline;
  }
  
  &:hover, &:focus {
    .tree__item__label {
      background-color: hsl(223, 74%, 91%);
      color: #1b47b7;
    }
  }
}

.tree__item {
  margin: 6px 0;
}

.tree__item__label {
  letter-spacing: 0.04em;
  padding: 4px 12px;
  border-radius: 6px;
  margin-left: 4px;
  color: #252525;
}
</style>
