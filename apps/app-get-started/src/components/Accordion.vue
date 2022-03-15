<template>
  <div :id="`accordion_${id}`" :class="accordionClass">
    <h3 class="heading">
      <button
        type="button"
        :id="`accordion_toggle_${id}`"
        class="toggle"
        :aria-controls="`accordion_content_${id}`"
        :aria-expanded="visible"
        v-on:click="onclick"
      >
        <span class="toggle_label">{{ title }}</span>
        <svg
          :class="iconClass"
          xmlns="http://www.w3.org/2000/svg"
          fill="none"
          width="24"
          height="24"
          viewBox="0 0 24 24"
          stroke="currentColor"
          stroke-width="2"
        >
          <path
            stroke-linecap="round"
            stroke-linejoin="round" d="M19 9l-7 7-7-7"
          />
        </svg>
      </button>
    </h3>
    <section
      :id="`accordion_content_${id}`"
      class="content"
      :aria-labelledby="`accordion_toggle_${id}`"
      role="region"
      v-show="visible"
    >
      <ul :id="`package_links_${id}`">
        <li v-for="link in links" :key="link.id">
          <a :href="`/menu/main/dataexplorer?entity=${link.id}&hideselect=true&mod=data`">
          {{ link.label }}
          </a>
        </li>
      </ul>
    </section>
  </div>
</template>

<script>
export default {
  name: 'Accordion',
  props: ['id', 'title', 'links'],
  data () {
    return {
      visible: false
    }
  },
  methods: {
    onclick () {
      this.visible = !this.visible
    }
  },
  computed: {
    accordionClass () {
      return `${this.visible ? 'accordion visible' : 'accordion'}`
    },
    iconClass () {
      return `${this.visible ? 'toggle_icon rotated' : 'toggle_icon'}`
    }
  }
}
</script>

<style lang="scss">
.accordion {
  font-family: inherit;
  box-sizing: border-box;
  margin: 24px 0;
  border: 1px solid #f6f6f6;
  border-radius: 6px;

  button {
    border: none;
    position: relative;
    background: none;
    background-color: none;
    margin: 0;
    padding: 0;
    cursor: pointer;
    font-size: inherit;
    text-align: left;
    color: currentColor;
  }

  .heading {
    display: flex;
    justify-content: flex-start;
    align-items: center;
    margin: 0;
    padding: 16px 12px;
    font-size: 14pt;
    background-color: #f6f6f6;

    .toggle {
      display: flex;
      justify-content: flex-start;
      align-items: center;
      width: 100%;

      $icon-size: 24px;
      .toggle_label {
        display: inline-block;
        width: calc(100% - $icon-size);
        word-break: break-word;
      }

      .toggle_icon {
        width: $icon-size;
        height: $icon-size;
        transform: rotate(0);
        transform-origin: center;
        transition: transform 0.4s ease-in-out;

        &.rotated {
          transform: rotate(180deg);
        }
      }
    }
  }

  .content {
    margin: 0;
    padding: 12px;
  }
}
</style>
