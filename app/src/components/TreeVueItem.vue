<template>
  <div v-if="children">
    <button
      :id="`toggle-${id}`"
      class="tree__toggle"
      @click="onclick"
      :aria-expanded="isOpen"
      :aria-controls="`subtree-${id}`"
    >
      <svg
        xmlns="http://www.w3.org/2000/svg"
        class="tree__item__icon"
        width="20"
        height="20"
        viewBox="0 0 20 20"
        fill="#DEAF02"
      >
        <path
          v-if="isOpen"
          fill-rule="evenodd"
          d="M4 4a2 2 0 00-2 2v8a2 2 0 002 2h12a2 2 0 002-2V8a2 2 0 00-2-2h-5L9 4H4zm4 6a1 1 0 100 2h4a1 1 0 100-2H8z"
        />
        <path
          v-else
          fill-rule="evenodd"
          d="M4 4a2 2 0 00-2 2v8a2 2 0 002 2h12a2 2 0 002-2V8a2 2 0 00-2-2h-5L9 4H4zm7 5a1 1 0 10-2 0v1H8a1 1 0 100 2h1v1a1 1 0 102 0v-1h1a1 1 0 100-2h-1V9z"
        />
      </svg>
      <span class="tree__item__label">{{ name }}</span>
    </button>
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
          :children="child.children"
        />
      </li>
    </ul>
  </div>
  <div v-else>
    <svg
      xmlns="http://www.w3.org/2000/svg"
      class="tree__item__icon"
      fill="none"
      width="18"
      height="18"
      viewBox="0 0 24 24"
      stroke="currentColor"
      stroke-width="2"
    >
      <path
        stroke-linecap="round"
        stroke-linejoin="round"
        d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
      />
    </svg>
    <span class="tree__item__label">{{ name }}</span>
  </div>
</template>

<script>
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
    children: {
      type: Array,
      required: false
    }
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
  .tree__item {
    position: relative;
    margin-left: 2px;
    margin-bottom: 4px;
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

.tree__item__icon {
  margin-bottom: 3px;
}

.tree__item__label {
  letter-spacing: 0.04em;
  padding: 4px 12px;
  border-radius: 6px;
  margin-left: 4px;
  color: #252525;
}
</style>
